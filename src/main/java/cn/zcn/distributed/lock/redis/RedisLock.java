package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.AbstractLock;
import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import io.netty.util.Timer;

class RedisLock extends AbstractLock {

    public RedisLock(String lock, String instanceId, Timer timer, Config config, LockSubscription lockSubscription) {
        super(lock, instanceId, timer, config, lockSubscription);
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return false;
    }

    @Override
    protected Long doLock(long durationMillis, long threadId) {
        return null;
    }

    @Override
    protected boolean doRenew(long durationMillis, long threadId) {
        return false;
    }

    @Override
    protected boolean doUnLock(long threadId) {
        return false;
    }
}
