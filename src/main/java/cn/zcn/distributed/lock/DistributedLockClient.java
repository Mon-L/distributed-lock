package cn.zcn.distributed.lock;

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

    public Lock getLock(String name) {
        return lockFactory.getLock(name);
    }
}
