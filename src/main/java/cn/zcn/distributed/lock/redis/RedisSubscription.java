package cn.zcn.distributed.lock.redis;

public interface RedisSubscription {

    /**
     * 订阅channel
     *
     * @param channels 待订阅待channels
     */
    void subscribe(RedisSubscriptionListener listener, byte[]... channels);

    /**
     * 订阅channel
     *
     * @param channels 待订阅待channels
     */
    void subscribe(byte[]... channels);

    /**
     * 取消订阅
     *
     * @param channels 待取消订阅待channels
     */
    void unsubscribe(byte[]... channels);

    long getSubscribedChannels();

    boolean isAlive();

    /**
     * 关闭并取消所有订阅
     */
    void close();
}
