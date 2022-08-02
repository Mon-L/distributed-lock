package cn.zcn.distributed.lock.redis.subscription;

public interface RedisSubscription {

    /**
     * 订阅 channels，设置订阅监听。
     * 该方法只需要被调用一次。随后使用 @{code subscribe(channels)} 增加订阅 channels
     *
     * @param listener 订阅监听
     * @param channels 订阅的 channels
     */
    void subscribe(RedisSubscriptionListener listener, byte[]... channels);

    /**
     * 订阅 channels
     *
     * @param channels 订阅的 channels
     */
    void subscribe(byte[]... channels);

    /**
     * 取消订阅
     *
     * @param channels 取消订阅的 channels
     */
    void unsubscribe(byte[]... channels);

    /**
     * 获取已订阅 channel 的数量
     *
     * @return 已订阅的channel数量
     */
    long getSubscribedChannels();

    /**
     * 是否处于订阅状态
     *
     * @return false，未订阅。true，正在订阅
     */
    boolean isAlive();

    /**
     * 关闭并取消所有订阅
     */
    void close();
}
