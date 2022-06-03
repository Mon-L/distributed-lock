package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.BinaryJedisPubSub;

class JedisPubSubAdapter extends BinaryJedisPubSub {

    private final RedisSubscriptionListener listener;

    JedisPubSubAdapter(RedisSubscriptionListener listener) {
        this.listener = listener;
    }

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