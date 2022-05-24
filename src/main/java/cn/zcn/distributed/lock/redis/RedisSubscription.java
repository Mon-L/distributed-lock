package cn.zcn.distributed.lock.redis;

public interface RedisSubscription {

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

    /**
     * 关闭并取消所有订阅
     */
    void close();
}
