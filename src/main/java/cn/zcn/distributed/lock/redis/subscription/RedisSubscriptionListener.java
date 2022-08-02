package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.LongEncoder;

public interface RedisSubscriptionListener {

    byte[] UNLOCK_MESSAGE = LongEncoder.encode(0L);

    void onMessage(byte[] channel, byte[] message);

    void onSubscribe(byte[] channel, long subscribedChannels);

    void onUnsubscribe(byte[] channel, long subscribedChannels);
}
