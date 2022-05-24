package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.BinaryJedisPubSub;

public class JedisSubscription implements RedisSubscription {

    private final BinaryJedisPubSub pubSub;
    private final RedisSubscriptionListener listener;

    public JedisSubscription(RedisSubscriptionListener listener) {
        this.listener = listener;
        this.pubSub = new JedisPubSubProxy();
    }

    public BinaryJedisPubSub getJedisPubSub() {
        return pubSub;
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
    public void close() {
        pubSub.unsubscribe();
    }

    private class JedisPubSubProxy extends BinaryJedisPubSub {

        @Override
        public void onMessage(byte[] channel, byte[] message) {
            listener.onMessage(channel, message);
        }

        @Override
        public void onSubscribe(byte[] channel, int subscribedChannels) {
            listener.onSubscribe(channel, subscribedChannels);
        }

        @Override
        public void onUnsubscribe(byte[] channel, int subscribedChannels) {
            listener.onUnsubscribe(channel, subscribedChannels);
        }
    }
}