package cn.zcn.distributed.lock.redis;

public interface RedisSubscriptionListener {
    void onMessage(byte[] channel, byte[] message);

    void onSubscribe(byte[] channel, int subscribedChannels);

    void onUnsubscribe(byte[] channel);
}
