package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.redis.RedisExecutor;
import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisSubscriptionService implements LockSubscriptionService {

    /**
     * 没有监听任何 channels
     */
    private static final int NOT_LISTEN = -1;

    /**
     * 准备监听 channels
     */
    private static final int PREPARE_LISTEN = 0;

    /**
     * 正在监听 channels
     */
    private static final int LISTENING = 1;

    /**
     * 订阅服务是否已启动
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 当前监听状态
     */
    private volatile int state = NOT_LISTEN;

    private final Timer timer;
    private final RedisExecutor redisExecutor;
    private final Map<ByteArrayHolder, LockStatusListener> channelListeners = new ConcurrentHashMap<>();

    private BlockingSubscriber subscriber;

    public RedisSubscriptionService(Timer timer, RedisExecutor redisExecutor) {
        this.timer = timer;
        this.redisExecutor = redisExecutor;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            DispatchLockStatusListener lockMessageListener = new DispatchLockStatusListener(channelListeners);

            if (redisExecutor.isBlocked()) {
                subscriber = new BlockingSubscriber(redisExecutor, lockMessageListener);
            } else {
                subscriber = new Subscriber(redisExecutor, lockMessageListener);
            }
        }
    }

    public int getSubscribedListeners() {
        return channelListeners.size();
    }

    public long getSubscribedChannels() {
        return subscriber.getSubscribedChannels();
    }

    private void tryListen() {
        if (!running.get()) {
            return;
        }

        if (channelListeners.size() > 0) {
            subscriber.init();
        }
    }

    @Override
    public CompletableFuture<Void> subscribe(String channel, LockStatusListener listener) {
        ByteArrayHolder channelHolder = new ByteArrayHolder(channel);
        CompletableFuture<Void> newPromise = new CompletableFuture<>();

        int curState = state;

        channelListeners.put(channelHolder, listener);
        subscriber.addSubscriptionPromise(channelHolder, newPromise);

        try {
            Timeout timeout = timeout(newPromise);

            newPromise.whenComplete((r, t) -> {
                timeout.cancel();

                //订阅异常
                if (t != null) {
                    channelListeners.remove(channelHolder);
                    subscriber.removeSubscriptionPromise(channelHolder, newPromise);
                }
            });

            tryListen();

            if (curState == LISTENING || curState == PREPARE_LISTEN) {
                subscriber.initPromise.whenComplete((r, t) -> {
                    if (newPromise.isDone()) {
                        return;
                    }

                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        return;
                    }

                    try {
                        subscriber.doSubscribe(channelHolder.val);
                    } catch (Exception e) {
                        newPromise.completeExceptionally(e);
                    }
                });
            }
        } catch (Throwable t) {
            newPromise.completeExceptionally(t);
        }

        return newPromise;
    }

    private Timeout timeout(CompletableFuture<Void> promise) {
        return timer.newTimeout(timeout -> {
            if (!promise.isDone()) {
                promise.completeExceptionally(new TimeoutException("Subscribe/UnSubscribe channel timeout."));
            }
        }, 5, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String channel) {
        ByteArrayHolder channelHolder = new ByteArrayHolder(channel);
        channelListeners.remove(channelHolder);

        if (state == NOT_LISTEN) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> newPromise = new CompletableFuture<>();
        Timeout timeout = timeout(newPromise);

        newPromise.whenComplete((r, t) -> timeout.cancel());

        subscriber.initPromise.whenComplete((r, t) -> {
            if (newPromise.isDone()) {
                return;
            }

            if (t != null) {
                newPromise.completeExceptionally(t);
                return;
            }

            try {
                subscriber.addUnsubscriptionPromise(channelHolder, newPromise);
                subscriber.doUnsubscribe(channelHolder.val);
            } catch (Exception e) {
                subscriber.removeUnsubscriptionPromise(channelHolder, newPromise);
                newPromise.completeExceptionally(e);
            }
        });

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

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            subscriber.shutdown();
        }
    }

    private class Subscriber extends BlockingSubscriber {

        private Subscriber(RedisExecutor redisExecutor, LockStatusListener messageListener) {
            super(redisExecutor, messageListener);
        }

        @Override
        protected void doSubscribe(RedisSubscriptionListener listener, byte[]... channel) {
            redisSubscription = redisExecutor.createSubscription();
            redisSubscription.subscribe(listener, channel);
        }
    }

    private class BlockingSubscriber implements RedisSubscriptionListener {

        private final RedisExecutor redisExecutor;
        private final LockStatusListener messageListener;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final Map<ByteArrayHolder, CompletableFuture<Void>> subscriptionPromises = new ConcurrentHashMap<>();
        private final Map<ByteArrayHolder, CompletableFuture<Void>> unsubscriptionPromises = new ConcurrentHashMap<>();

        private volatile CompletableFuture<Void> initPromise;
        protected volatile RedisSubscription redisSubscription;

        private BlockingSubscriber(RedisExecutor redisExecutor, LockStatusListener messageListener) {
            this.redisExecutor = redisExecutor;
            this.messageListener = messageListener;
        }

        @Override
        public void onMessage(byte[] channel, byte[] message) {
            if (Arrays.equals(message, UNLOCK_MESSAGE)) {
                messageListener.unlock(new String(channel), message);
            }
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
            CompletableFuture<Void> promise = unsubscriptionPromises.remove(new ByteArrayHolder(channel));
            if (promise != null) {
                promise.complete(null);
            }
        }

        private void addSubscriptionPromise(ByteArrayHolder channel, CompletableFuture<Void> promise) {
            subscriptionPromises.put(channel, promise);
        }

        private void removeSubscriptionPromise(ByteArrayHolder channel, CompletableFuture<Void> promise) {
            subscriptionPromises.remove(channel, promise);
        }

        private void addUnsubscriptionPromise(ByteArrayHolder channel, CompletableFuture<Void> promise) {
            unsubscriptionPromises.put(channel, promise);
        }

        private void removeUnsubscriptionPromise(ByteArrayHolder channel, CompletableFuture<Void> promise) {
            unsubscriptionPromises.remove(channel, promise);
        }

        private long getSubscribedChannels() {
            return redisSubscription == null ? 0 : redisSubscription.getSubscribedChannels();
        }

        private void init() {
            if (state != NOT_LISTEN) {
                return;
            }

            synchronized (this) {
                if (state != NOT_LISTEN) {
                    return;
                }

                initPromise = new CompletableFuture<>();
                initPromise.whenComplete((r, t) -> {
                    if (t != null) {
                        state = NOT_LISTEN;
                        return;
                    }
                    state = LISTENING;
                });

                try {
                    Set<ByteArrayHolder> channels = new HashSet<>(channelListeners.keySet());
                    state = PREPARE_LISTEN;
                    doSubscribe(this, unwrap(channels));
                } catch (Exception e) {
                    handleSubscriptionException(e);
                }
            }
        }

        private void handleSubscriptionException(Exception e) {
            state = NOT_LISTEN;
            initPromise.completeExceptionally(e);

            if (running.get()) {
                sleepBeforeReconnect();
                tryListen();
            }
        }

        protected void doSubscribe(RedisSubscriptionListener listener, byte[]... channel) {
            executor.execute(() -> {
                if (!running.get()) {
                    return;
                }

                try {
                    redisSubscription = redisExecutor.createSubscription();
                    redisSubscription.subscribe(listener, channel);
                    state = NOT_LISTEN;
                } catch (Exception e) {
                    handleSubscriptionException(e);
                }
            });
        }

        private void doSubscribe(byte[] channel) {
            if (channel != null && channel.length > 0) {
                synchronized (this) {
                    redisSubscription.subscribe(channel);
                }
            }
        }

        private void doUnsubscribe(byte[] channel) {
            if (channel != null && channel.length > 0) {
                synchronized (this) {
                    redisSubscription.unsubscribe(channel);
                }
            }
        }

        private void sleepBeforeReconnect() {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void shutdown() {
            if (redisSubscription != null && redisSubscription.isAlive()) {
                redisSubscription.close();
            }
            executor.shutdown();
        }
    }

    private static class DispatchLockStatusListener implements LockStatusListener {

        private final Map<ByteArrayHolder, LockStatusListener> listeners;

        private DispatchLockStatusListener(Map<ByteArrayHolder, LockStatusListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void unlock(String channel, Object message) {
            LockStatusListener l = listeners.get(new ByteArrayHolder(channel));
            if (l != null) {
                l.unlock(channel, message);
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
