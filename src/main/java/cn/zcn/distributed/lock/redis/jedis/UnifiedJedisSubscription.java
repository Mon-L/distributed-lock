package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.subscription.AbstractRedisSubscription;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionListener;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.UnifiedJedis;

public class UnifiedJedisSubscription extends AbstractRedisSubscription {
    private final UnifiedJedis jedis;
    private BinaryJedisPubSub pubSub;

    UnifiedJedisSubscription(UnifiedJedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public void doSubscribe(RedisSubscriptionListener listener, byte[]... channels) {
        this.pubSub = new JedisPubSubAdapter(listener);
        jedis.subscribe(pubSub, channels);
    }

    @Override
    public void doSubscribe(byte[]... channels) {
        pubSub.subscribe(channels);
    }

    @Override
    public void doUnsubscribe(byte[]... channels) {
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