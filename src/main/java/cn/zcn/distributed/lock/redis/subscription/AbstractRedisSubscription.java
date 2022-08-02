package cn.zcn.distributed.lock.redis.subscription;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractRedisSubscription implements RedisSubscription {

    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);

    @Override
    public void subscribe(RedisSubscriptionListener listener, byte[]... channels) {
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("Channels must not be null or empty.");
        }

        if (listener == null) {
            throw new IllegalArgumentException("Subscription listener must not be null.");
        }

        if (isSubscribed.get()) {
            throw new IllegalStateException("Already subscribed. use subscribe(channels) to add new channels.");
        }

        if (isSubscribed.compareAndSet(false, true)) {
            doSubscribe(listener, channels);
        } else {
            subscribe(listener, channels);
        }
    }

    @Override
    public void subscribe(byte[]... channels) {
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("Channels must not be null or empty.");
        }

        doSubscribe(channels);
    }

    @Override
    public void unsubscribe(byte[]... channels) {
        if (channels == null || channels.length == 0) {
            throw new IllegalArgumentException("Channels must not be null or empty.");
        }

        doUnsubscribe(channels);
    }

    protected abstract void doSubscribe(RedisSubscriptionListener listener, byte[]... channels);

    protected abstract void doSubscribe(byte[]... channels);

    protected abstract void doUnsubscribe(byte[]... channels);
}
