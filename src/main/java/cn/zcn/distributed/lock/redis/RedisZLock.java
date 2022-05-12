package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.BaseZLock;
import cn.zcn.distributed.lock.SubscribeLatch;
import io.netty.util.Timer;

public class RedisZLock extends BaseZLock {

    public RedisZLock(String lock, String clientId, Timer timer) {
        super(lock, clientId, timer);
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return false;
    }

    @Override
    public SubscribeLatch subscribe(String entry) {
        return null;
    }

    @Override
    public void unSubscribe(String entry) {

    }

    @Override
    public Long doLock(long durationMillis, String entry) {
        return null;
    }

    @Override
    public long doRenew(String entry) {
        return 0;
    }

    @Override
    public void doUnLock(String entry) {

    }
}
