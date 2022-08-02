package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.subscription.AbstractRedisSubscription;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionListener;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;

class JedisSubscription extends AbstractRedisSubscription {

    private final Jedis jedis;
    private BinaryJedisPubSub pubSub;

    JedisSubscription(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    protected void doSubscribe(RedisSubscriptionListener listener, byte[]... channels) {
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
        if (jedis.isConnected()) {
            if (isAlive()) {
                pubSub.unsubscribe();
            }

            jedis.close();
        }
    }
}