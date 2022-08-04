package cn.zcn.distributed.lock.zookeeper;

public interface ZkReadWriteLock {

    ZkLock readLock();

    ZkLock writeLock();
}
