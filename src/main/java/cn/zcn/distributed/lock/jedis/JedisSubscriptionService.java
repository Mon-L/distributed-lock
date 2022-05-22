package cn.zcn.distributed.lock.jedis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import cn.zcn.distributed.lock.subscription.SerialRunnableQueen;
import cn.zcn.distributed.lock.subscription.SubscriptionListener;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class JedisSubscriptionService implements LockSubscriptionService {

    private final Config config;
    private final Timer timer;
    private final JedisPool jedisPool;

    /**
     * 标识jedis订阅服务是否正在允许
     */
    private volatile boolean running = false;

    /**
     * 标识jedis是否在订阅主题中。订阅未开始或者连接断开后listening为false
     */
    private volatile boolean listening = false;
    private final SubscriptionTask subscriptionTask = new SubscriptionTask();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SerialRunnableQueen queen = new SerialRunnableQueen();
    private final Map<ByteArrayWrapper, SubscriptionListener> listeners = new ConcurrentHashMap<>();
    private final Map<ByteArrayWrapper, CompletableFuture<Void>> subscribing = new ConcurrentHashMap<>();
    private final DispatchSubscriptionListener dispatchMessageListener = new DispatchSubscriptionListener();
    private BinaryJedisPubSub jedisPubSub;

    private class DispatchSubscriptionListener implements SubscriptionListener {
        @Override
        public void onMessage(String channel, Object message) {
            SubscriptionListener l = listeners.get(new ByteArrayWrapper(channel));
            if (l != null) {
                l.onMessage(channel, message);
            }
        }
    }

    private static class ByteArrayWrapper {
        private final byte[] val;

        private ByteArrayWrapper(String str) {
            this.val = str.getBytes(StandardCharsets.UTF_8);
        }

        private ByteArrayWrapper(byte[] val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteArrayWrapper)) return false;

            ByteArrayWrapper that = (ByteArrayWrapper) o;

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

    @Override
    public void start() {
        if (!running) {
            running = true;
            jedisPubSub = new BinaryJedisPubSub() {
                @Override
                public void onMessage(byte[] channel, byte[] message) {
                    dispatchMessageListener.onMessage(new String(channel), message);
                }

                @Override
                public void onSubscribe(byte[] channel, int subscribedChannels) {
                    CompletableFuture<Void> promise = subscribing.remove(new ByteArrayWrapper(channel));
                    if (promise != null) {
                        promise.complete(null);
                    }
                }
            };

            //启动订阅任务
            executor.execute(subscriptionTask);
        }
    }

    @Override
    public CompletableFuture<Void> subscribe(String channel, SubscriptionListener listener) {
        CompletableFuture<Void> newPromise = new CompletableFuture<>();
        ByteArrayWrapper wrapper = new ByteArrayWrapper(channel);

        newPromise.whenComplete((r, t) -> {
            //订阅异常或超时
            if (t != null) {
                listeners.remove(wrapper);
                subscribing.remove(wrapper);
            }
        });

        queen.add(() -> {
            if (newPromise.isDone()) {
                queen.runNext();
                return;
            }

            listeners.put(wrapper, listener);
            subscribing.put(wrapper, newPromise);

            if (listening) {
                try {
                    jedisPubSub.subscribe(wrapper.val);
                } catch (Exception e) {
                    newPromise.completeExceptionally(e);
                }
            }

            queen.runNext();
        });

        timeout(newPromise, "Subscribe channel timeout.");

        return newPromise;
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String channel) {
        CompletableFuture<Void> newPromise = new CompletableFuture<>();
        ByteArrayWrapper wrapper = new ByteArrayWrapper(channel);

        queen.add(() -> {
            if (newPromise.isDone()) {
                queen.runNext();
                return;
            }

            listeners.remove(wrapper);

            if (listening) {
                try {
                    jedisPubSub.unsubscribe(wrapper.val);
                    newPromise.complete(null);
                } catch (Exception e) {
                    newPromise.completeExceptionally(e);
                }
            }

            queen.runNext();
        });

        timeout(newPromise, "Unsubscribe channel timeout.");

        return newPromise;
    }

    private byte[][] unwrap(Set<ByteArrayWrapper> keys) {
        if (keys.isEmpty()) {
            return new byte[0][0];
        }

        byte[][] channels = new byte[keys.size()][];
        Iterator<ByteArrayWrapper> iter = keys.iterator();
        int i = 0;
        while (iter.hasNext()) {
            channels[i++] = iter.next().val;
        }

        return channels;
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
    public void stop() {
        if (running) {
            running = false;
            subscriptionTask.shutdown();
            executor.shutdown();
        }
    }

    private class SubscriptionTask implements Runnable {

        private Jedis jedis;

        @Override
        public void run() {
            while (running) {
                try {
                    jedis = jedisPool.getResource();
                    listening = true;
                    jedis.subscribe(jedisPubSub, unwrap(listeners.keySet()));
                } catch (Exception e) {
                    listening = false;
                    if (jedis != null) {
                        jedis.close();
                    }

                    if (running) {
                        sleepBeforeReconnect();
                    }
                }
            }
        }

        private void sleepBeforeReconnect() {
            try {
                Thread.sleep(config.getReconnectInterval());
            } catch (InterruptedException interEx) {
                Thread.currentThread().interrupt();
            }
        }

        private void shutdown() {
            listening = false;
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
