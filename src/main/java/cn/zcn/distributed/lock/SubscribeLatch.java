package cn.zcn.distributed.lock;

import java.util.concurrent.TimeUnit;

public interface SubscribeLatch {

    void tryAcquire(long wait, TimeUnit unit) throws InterruptedException;

    void acquire() throws InterruptedException;
}
