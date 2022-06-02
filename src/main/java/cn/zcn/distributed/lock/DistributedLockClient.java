package cn.zcn.distributed.lock;

public class DistributedLockClient {

    public static JedisDistributedLockClientBuilder withJedis() {
        return new JedisDistributedLockClientBuilder();
    }

    public static LettuceDistributedLockClientBuilder withLettuce() {
        return new LettuceDistributedLockClientBuilder();
    }

    private final DistributedLockCreator lockCreatorImpl;

    DistributedLockClient(DistributedLockCreator distributedLockCreatorImpl) {
        this.lockCreatorImpl = distributedLockCreatorImpl;
    }

    public Lock getLock(String name) {
        return lockCreatorImpl.getLock(name);
    }

    public void shutdown() {
        lockCreatorImpl.shutdown();
    }
}
