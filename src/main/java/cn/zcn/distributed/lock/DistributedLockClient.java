package cn.zcn.distributed.lock;

/**
 * 分布式锁客户端，用于初始化分布式锁环境和获取分布式锁
 */
public class DistributedLockClient {

    public static JedisDistributedLockClientBuilder withJedis() {
        return new JedisDistributedLockClientBuilder();
    }

    public static LettuceDistributedLockClientBuilder withLettuce() {
        return new LettuceDistributedLockClientBuilder();
    }

    private final LockFactory lockFactory;

    DistributedLockClient(LockFactory lockFactoryImpl) {
        this.lockFactory = lockFactoryImpl;
    }

    /**
     * 获取非公平锁
     *
     * @param name 锁名
     * @return 非公平锁
     */
    public Lock getLock(String name) {
        return lockFactory.getLock(name);
    }

    /**
     * 获取公平锁
     *
     * @param name 锁名
     * @return 公平锁
     */
    public Lock getFairLock(String name) {
        return lockFactory.getFairLock(name);
    }
}
