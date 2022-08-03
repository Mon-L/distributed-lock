package cn.zcn.distributed.lock.zookeeper;

public interface ZookeeperReadWriteLock {

    ZookeeperLock readLock();

    ZookeeperLock writeLock();
}
