package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.redis.subscription.RedisSubscription;

import java.util.List;

public interface RedisExecutor {

    /**
     * 执行 redis lua 脚本
     *
     * @param script lua 脚本
     * @param keys   所用到的 redis 键，
     * @param args   所用到的 redis 值
     * @return 脚本运行结果
     */
    Object eval(byte[] script, List<byte[]> keys, List<byte[]> args);

    /**
     * 获取可执行订阅的对象
     *
     * @return {@link RedisSubscription}
     */
    RedisSubscription createSubscription();

    /**
     * 是否阻塞型的 Redis client
     *
     * @return {@code true},是阻塞型;{@code false},不是阻塞型;
     */
    boolean isBlocked();

    void stop();
}
