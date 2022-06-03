package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.UnifiedJedis;

import java.util.concurrent.atomic.AtomicBoolean;

public class UnifiedJedisSubscription implements RedisSubscription {
    private final UnifiedJedis jedis;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private BinaryJedisPubSub pubSub;

    UnifiedJedisSubscription(UnifiedJedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public void subscribe(RedisSubscriptionListener listener, byte[]... channels) {
        if (isSubscribed.get()) {
            throw new IllegalStateException("Already subscribed. use subscribe(channels) to add new channels.");
        }

        if (isSubscribed.compareAndSet(false, true)) {
            this.pubSub = new JedisPubSubAdapter(listener);
            jedis.subscribe(pubSub, channels);
        } else {
            subscribe(listener, channels);
        }
    }

    @Override
    public void subscribe(byte[]... channels) {
        pubSub.subscribe(channels);
    }

    @Override
    public void unsubscribe(byte[]... channels) {
        pubSub.unsubscribe(channels);
    }

    @Override
    public long getSubscribedChannels() {
        return pubSub.getSubscribedChannels();
    }

    @Override
    public boolean isAlive() {
        return pubSub.isSubscribed();
    }

    @Override
    public void close() {
        if (isAlive()) {
            pubSub.unsubscribe();
        }
    }
}