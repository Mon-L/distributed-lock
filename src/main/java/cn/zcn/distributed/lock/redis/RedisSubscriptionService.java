package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.subscription.LockMessageListener;
import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisSubscriptionService implements LockSubscriptionService {

    /**
     * 没有监听任何channel
     */
    private static final int NOT_LISTEN = -1;

    /**
     * 正在监听chanel
     */
    private static final int LISTENING = 1;

    /**
     * redis订阅服务是否已经运行
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 当前监听状态
     */
    private volatile int state = NOT_LISTEN;

    private final Config config;
    private final Timer timer;
    private final RedisCommandFactory commandFactory;
    private final Map<ByteArrayHolder, LockMessageListener> channelListeners = new HashMap<>();
    private BlockingSubscriptionTask subscriptionTask;

    public RedisSubscriptionService(Config config, RedisCommandFactory commandFactory, Timer timer) {
        this.timer = timer;
        this.config = config;
        this.commandFactory = commandFactory;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            DispatchLockMessageListener lockMessageListener = new DispatchLockMessageListener(channelListeners);
            subscriptionTask = new BlockingSubscriptionTask(commandFactory, lockMessageListener);
        }
    }

    public int getSubscribedListeners() {
        return channelListeners.size();
    }

    public long getSubscribedChannels() {
        RedisSubscription redisSubscription = commandFactory.getSubscription();
        return redisSubscription != null ? redisSubscription.getSubscribedChannels() : 0;
    }

    private void tryListen() {
        if (state == LISTENING) {
            return;
        }

        if (running.get()) {
            if (state <= 0 && channelListeners.size() > 0) {
                CompletableFuture<Void> initPromise = subscriptionTask.init();

                try {
                    initPromise.get(config.getTimeout(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    throw new CompletionException(e.getCause());
                } catch (TimeoutException e) {
                    throw new IllegalStateException("Subscription registration timeout exceeded.", e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> subscribe(String channel, LockMessageListener listener) {
        ByteArrayHolder channelHolder = new ByteArrayHolder(channel);
        CompletableFuture<Void> newPromise = new CompletableFuture<>();

        int curState = state;
        channelListeners.put(channelHolder, listener);

        newPromise.whenComplete((r, t) -> {
            //订阅异常或超时
            if (t != null) {
                channelListeners.remove(channelHolder);
                subscriptionTask.removeSubscriptionPromise(channelHolder, newPromise);
            }
        });

        try {
            if (curState == NOT_LISTEN) {
                subscriptionTask.addSubscriptionPromise(channelHolder, newPromise);
            }

            tryListen();

            if (curState == LISTENING) {
                subscriptionTask.addSubscriptionPromise(channelHolder, newPromise);
                subscriptionTask.doSubscribe(channelHolder.val);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            newPromise.completeExceptionally(t);
        }

        timeout(newPromise, "Subscribe channel timeout.");
        return newPromise;
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String channel) {
        ByteArrayHolder channelHolder = new ByteArrayHolder(channel);

        int curState = state;
        channelListeners.remove(channelHolder);

        if (curState == NOT_LISTEN) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> newPromise = new CompletableFuture<>();

        try {
            subscriptionTask.doUnsubscribe(channelHolder.val);
            newPromise.complete(null);
        } catch (Exception e) {
            newPromise.completeExceptionally(e);
        }

        timeout(newPromise, "Unsubscribe channel timeout.");
        return newPromise;
    }

    private byte[][] unwrap(Set<ByteArrayHolder> channelHolders) {
        if (channelHolders.isEmpty()) {
            return new byte[0][0];
        }

        byte[][] channels = new byte[channelHolders.size()][];
        Iterator<ByteArrayHolder> iter = channelHolders.iterator();
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
        if (running.compareAndSet(true, false)) {
            subscriptionTask.shutdown();
        }
    }

    private class BlockingSubscriptionTask implements RedisSubscriptionListener {

        private final RedisCommandFactory commandFactory;
        private final LockMessageListener messageListener;
        private volatile CompletableFuture<Void> initPromise;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final Map<ByteArrayHolder, CompletableFuture<Void>> subscriptionPromises = new ConcurrentHashMap<>();

        private BlockingSubscriptionTask(RedisCommandFactory commandFactory, LockMessageListener messageListener) {
            this.commandFactory = commandFactory;
            this.messageListener = messageListener;
        }

        @Override
        public void onMessage(byte[] channel, byte[] message) {
            messageListener.onMessage(new String(channel), message);
        }

        @Override
        public void onSubscribe(byte[] channel, long subscribedChannels) {
            if (initPromise != null && !initPromise.isDone()) {
                initPromise.complete(null);
            }

            CompletableFuture<Void> promise = subscriptionPromises.remove(new ByteArrayHolder(channel));
            if (promise != null) {
                promise.complete(null);
            }
        }

        @Override
        public void onUnsubscribe(byte[] channel, long subscribedChannels) {
        }

        private CompletableFuture<Void> getSubscriptionPromise(ByteArrayHolder channel) {
            return subscriptionPromises.get(channel);
        }

        private void addSubscriptionPromise(ByteArrayHolder channel, CompletableFuture<Void> promise) {
            subscriptionPromises.put(channel, promise);
        }

        private void removeSubscriptionPromise(ByteArrayHolder channel, CompletableFuture<Void> promise) {
            subscriptionPromises.remove(channel, promise);
        }

        private CompletableFuture<Void> init() {
            if (state != NOT_LISTEN) {
                return initPromise;
            }

            synchronized (this) {
                if (state != NOT_LISTEN) {
                    return initPromise;
                }

                initPromise = new CompletableFuture<>();
                initPromise.whenComplete((r, t) -> {
                    if (t == null) {
                        state = LISTENING;
                    }
                });

                Set<ByteArrayHolder> channels = new HashSet<>(channelListeners.keySet());

                executor.execute(() -> {
                    if (!running.get()) {
                        return;
                    }

                    try {
                        doSubscribe(BlockingSubscriptionTask.this, unwrap(channels));
                        state = NOT_LISTEN;
                    } catch (Throwable t) {
                        state = NOT_LISTEN;
                        initPromise.completeExceptionally(t);

                        if (running.get()) {
                            sleepBeforeReconnect();
                            tryListen();
                        }
                    }
                });

                return initPromise;
            }
        }

        private void doSubscribe(RedisSubscriptionListener listener, byte[]... channel) {
            commandFactory.subscribe(listener, channel);
        }

        private void doSubscribe(byte[] channel) {
            if (channel.length > 0) {
                synchronized (this) {
                    RedisSubscription subscription = commandFactory.getSubscription();
                    if (subscription != null) {
                        subscription.subscribe(channel);
                    }
                }
            }
        }

        private void doUnsubscribe(byte[] channel) {
            if (channel.length > 0) {
                synchronized (this) {
                    RedisSubscription subscription = commandFactory.getSubscription();
                    if (subscription != null) {
                        subscription.unsubscribe(channel);
                    }
                }
            }
        }

        private void doUnsubscribeAll() {
            synchronized (this) {
                RedisSubscription subscription = commandFactory.getSubscription();
                if (subscription != null) {
                    subscription.close();
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
            doUnsubscribeAll();
            executor.shutdown();
        }
    }

    private static class DispatchLockMessageListener implements LockMessageListener {

        private final Map<ByteArrayHolder, LockMessageListener> listeners;

        private DispatchLockMessageListener(Map<ByteArrayHolder, LockMessageListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onMessage(String channel, Object message) {
            LockMessageListener l = listeners.get(new ByteArrayHolder(channel));
            if (l != null) {
                l.onMessage(channel, message);
            }
        }
    }

    private static class ByteArrayHolder {
        private final byte[] val;

        private ByteArrayHolder(String str) {
            this.val = str.getBytes(StandardCharsets.UTF_8);
        }

        private ByteArrayHolder(byte[] val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteArrayHolder)) return false;

            ByteArrayHolder that = (ByteArrayHolder) o;

            return Arrays.equals(val, that.val);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(val);
        }
    }
}
