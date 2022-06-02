package cn.zcn.distributed.lock;

public interface DistributedLockCreator {
    void start();

    Lock getLock(String name);

    void shutdown();
}
