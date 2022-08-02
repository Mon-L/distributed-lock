package cn.zcn.distributed.lock.zookeeper;

import java.util.concurrent.TimeUnit;

public interface ZookeeperLock {

    void lock() throws Exception;

    boolean tryLock(long waitTime, TimeUnit waitTimeUnit) throws Exception;

    void unlock() throws Exception;

    boolean heldByCurrentThread();
}
