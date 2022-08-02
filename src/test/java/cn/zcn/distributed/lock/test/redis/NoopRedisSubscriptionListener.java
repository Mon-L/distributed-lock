package cn.zcn.distributed.lock.test.redis;

import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionListener;

public class NoopRedisSubscriptionListener implements RedisSubscriptionListener {

    @Override
    public void onMessage(byte[] channel, byte[] message) {
        
    }

    @Override
    public void onSubscribe(byte[] channel, long subscribedChannels) {

    }

    @Override
    public void onUnsubscribe(byte[] channel, long subscribedChannels) {

    }
}
