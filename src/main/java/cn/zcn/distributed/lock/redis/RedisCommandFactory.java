package cn.zcn.distributed.lock.redis;

import java.util.List;

public interface RedisCommandFactory {

    /**
     * 执行lua脚本
     *
     * @param script lua 脚本
     * @param keys   redis keys
     * @param args   参数
     * @return 脚本执行结果
     */
    Object eval(byte[] script, List<byte[]> keys, List<byte[]> args);

    /**
     * 订阅channel
     *
     * @param listener 订阅监听器
     * @param channels 待订阅待channels
     */
    void subscribe(RedisSubscriptionListener listener, byte[]... channels);

    /**
     * 返回订阅器，有且仅有一个
     *
     * @return null, 没有任何订阅
     */
    RedisSubscription getSubscription();
}
