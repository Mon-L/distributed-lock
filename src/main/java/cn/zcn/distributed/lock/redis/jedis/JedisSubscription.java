package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

import java.util.concurrent.atomic.AtomicBoolean;

class JedisSubscription implements RedisSubscription {

    private final Jedis jedis;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private BinaryJedisPubSub pubSub;

    JedisSubscription(Jedis jedis) {
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
    public void subscribe(byte[]... channel) {
        pubSub.subscribe(channel);
    }

    @Override
    public void unsubscribe(byte[]... channel) {
        pubSub.unsubscribe(channel);
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
        if (jedis.isConnected()) {
            if (isAlive()) {
                pubSub.unsubscribe();
            }

            jedis.close();
        }
    }
}