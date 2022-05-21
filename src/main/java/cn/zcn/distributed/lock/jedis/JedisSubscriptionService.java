package cn.zcn.distributed.lock.jedis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import cn.zcn.distributed.lock.subscription.SubscriptionListener;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class JedisSubscriptionService implements LockSubscriptionService {

    private final Config config;
    private final Timer timer;
    private final JedisPool jedisPool;
    private final DispatchSubscriptionListener dispatchMessageListener = new DispatchSubscriptionListener();
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Map<ByteChannelHolder, CompletableFuture<Void>> subscribingChannels = new ConcurrentHashMap<>();
    private final Map<ByteChannelHolder, CompletableFuture<Void>> unsubscribingChannels = new ConcurrentHashMap<>();
    private final Map<ByteChannelHolder, SubscriptionListener> listeners = new ConcurrentHashMap<>();

    private BinaryJedisPubSub pubSub;
    private ExecutorService executor;
    private CompletableFuture<Boolean> runningPromise = new CompletableFuture<>();

    private class DispatchSubscriptionListener implements SubscriptionListener {
        @Override
        public void onMessage(String channel, Object message) {
            SubscriptionListener listener = listeners.get(new ByteChannelHolder(channel));
            if (listener != null) {
                listener.onMessage(channel, message);
            }
        }
    }

    private static class ByteChannelHolder {
        private final byte[] val;

        private ByteChannelHolder(String channel) {
            this.val = channel.getBytes(StandardCharsets.UTF_8);
        }

        private ByteChannelHolder(byte[] channel) {
            this.val = channel;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteChannelHolder)) return false;

            ByteChannelHolder that = (ByteChannelHolder) o;

            return Arrays.equals(val, that.val);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(val);
        }
    }

    public JedisSubscriptionService(Config config, JedisPool jedisPool, Timer timer) {
        this.config = config;
        this.timer = timer;
        this.jedisPool = jedisPool;
    }

    public void init() {
        pubSub = new BinaryJedisPubSub() {
            @Override
            public void onMessage(byte[] channel, byte[] message) {
                dispatchMessageListener.onMessage(new String(channel), message);
            }

            @Override
            public void onSubscribe(byte[] channel, int subscribedChannels) {
                CompletableFuture<Void> promise = subscribingChannels.get(new ByteChannelHolder(channel));
                if (promise != null) {
                    promise.complete(null);
                }
            }

            @Override
            public void onUnsubscribe(byte[] channel, int subscribedChannels) {
                CompletableFuture<Void> promise = unsubscribingChannels.get(new ByteChannelHolder(channel));
                if (promise != null) {
                    promise.complete(null);
                }
            }
        };

        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public CompletableFuture<Void> subscribe(String channel, SubscriptionListener listener) {
        ByteChannelHolder channelHolder = new ByteChannelHolder(channel);
        CompletableFuture<Void> newPromise = new CompletableFuture<>();
        listeners.put(channelHolder, listener);
        subscribingChannels.put(channelHolder, newPromise);

        newPromise.whenComplete((r, t) -> {
            subscribingChannels.remove(channelHolder);

            if (t != null) {
                listeners.remove(channelHolder);
            }
        });

        while (true) {
            if (isRunning.get()) {
                runningPromise.whenComplete((r, t) -> {
                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        return;
                    }

                    try {
                        pubSub.subscribe(channelHolder.val);
                    } catch (JedisConnectionException e) {
                        newPromise.completeExceptionally(e);
                    }
                });
                break;
            } else {
                if (isRunning.compareAndSet(false, true)) {
                    start(channelHolder, newPromise);
                    break;
                }
            }
        }

        timeout(newPromise, "Subscribe lock timeout.");

        return newPromise;
    }

    private void start(ByteChannelHolder channel, CompletableFuture<Void> newPromise) {
        executor.execute(() -> {
            try {
                //TODO 如何选择一条可用连接
                Jedis jedis = jedisPool.getResource();
                jedis.subscribe(pubSub, channel.val);
            } catch (Exception e) {
                if (!newPromise.isDone()) {
                    newPromise.completeExceptionally(e);
                }

                //TODO 处理断连情况
                if (e instanceof JedisConnectionException) {
                    isRunning.set(false);
                    runningPromise = new CompletableFuture<>();
                }
            }
        });

        newPromise.whenComplete((r, t) -> {
            if (t != null) {
                runningPromise.completeExceptionally(t);
                return;
            }
            runningPromise.complete(true);
        });
    }

    private void timeout(CompletableFuture<Void> promise, String err) {
        Timeout timeout = timer.newTimeout(t -> {
            if (!promise.isDone()) {
                promise.completeExceptionally(new TimeoutException(err));
            }
        }, config.getTimeout(), TimeUnit.MILLISECONDS);

        promise.whenComplete((r, t) -> {
            if (!timeout.isCancelled()) {
                timeout.cancel();
            }
        });
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String channel) {
        ByteChannelHolder channelHolder = new ByteChannelHolder(channel);
        CompletableFuture<Void> newPromise = new CompletableFuture<>();

        if (isRunning.get()) {
            unsubscribingChannels.put(channelHolder, newPromise);
            newPromise.whenComplete((r, t) -> {
                unsubscribingChannels.remove(channelHolder);
            });

            listeners.remove(channelHolder);

            try {
                pubSub.unsubscribe(channelHolder.val);
                timeout(newPromise, "Unsubscribe lock timeout.");
            } catch (JedisConnectionException e) {
                newPromise.completeExceptionally(e);
            }
        } else {
            newPromise.complete(null);
        }

        return newPromise;
    }

    @Override
    public void close() {

    }
}
