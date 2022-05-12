package cn.zcn.distributed.lock;

import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁的结构
 * -lock-{lock name} = {
 * {UUID}:{thread id} = {lock count}
 * }
 */
public abstract class BaseZLock implements ZLock {

    private final static String LOCK_PREFIX = "-lock-";

    protected final String lock;
    protected final String instanceId;
    private final Timer renewTimer;
    private Timeout renewTimeout;

    /**
     * @param lock       分布式锁的名称
     * @param instanceId UUID
     * @param renewTimer 锁续期定时器
     */
    public BaseZLock(String lock, String instanceId, Timer renewTimer) {
        this.lock = LOCK_PREFIX + lock;
        this.instanceId = instanceId;
        this.renewTimer = renewTimer;
    }

    @Override
    public void lock(long duration, TimeUnit durationTimeUnit) throws InterruptedException {
        String lockEntry = getLockEntry();
        long durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, lockEntry);
        if (ttl == null) {
            //获取锁成功
            return;
        }

        //获取锁失败，订阅锁的状态
        SubscribeLatch subLatch = subscribe(getLockEntry());

        try {
            while (true) {
                //尝试获取锁
                ttl = innerLock(durationMillis, lockEntry);
                if (ttl == null) {
                    //获取锁成功
                    return;
                }

                if (ttl >= 0) {
                    //锁已被其他线程锁定，需等待 ttl 毫秒
                    subLatch.tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    // 获取锁失败，但锁已过期
                    subLatch.acquire();
                }
            }
        } finally {
            unSubscribe(getLockEntry());
        }
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit waitTimeUnit, long duration, TimeUnit durationTimeUnit) throws InterruptedException {
        String lockEntry = getLockEntry();
        long startMillis = System.currentTimeMillis();
        long waitTimeMillis = waitTimeUnit.toMillis(waitTime);
        long durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, lockEntry);
        if (ttl == null) {
            //获取锁成功
            return true;
        }

        if (System.currentTimeMillis() - startMillis >= waitTimeMillis) {
            return false;
        }

        SubscribeLatch subLatch = subscribe(getLockEntry());

        try {
            while (true) {
                if (System.currentTimeMillis() - startMillis >= waitTimeMillis) {
                    return false;
                }

                //尝试获取锁
                ttl = innerLock(durationMillis, lockEntry);
                if (ttl == null) {
                    //获取锁成功
                    return true;
                }

                if (ttl >= 0) {
                    subLatch.tryAcquire(System.currentTimeMillis() - startMillis, TimeUnit.MILLISECONDS);
                } else {
                    // 获取锁失败，但锁已过期
                    subLatch.acquire();
                }
            }
        } finally {
            unSubscribe(getLockEntry());
        }
    }

    private Long innerLock(long durationMillis, String lockEntry) {
        Long ttl = doLock(durationMillis, lockEntry);
        setRenewTimer(ttl, lockEntry);
        return ttl;
    }

    private void setRenewTimer(long durationMillis, String lockEntry) {
        this.renewTimeout = renewTimer.newTimeout(t -> {
            if (t.isCancelled()) {
                return;
            }

            long ttl = doRenew(lockEntry);
            if (ttl > 0) {
                setRenewTimer(ttl, lockEntry);
            } else {
                t.cancel();
            }
        }, durationMillis / 3, TimeUnit.MILLISECONDS);
    }

    @Override
    public void unlock() {
        if (isHeldByCurrentThread()) {
            if (!renewTimeout.isCancelled()) {
                renewTimeout.cancel();
            }
            doUnLock(getLockEntry());
        }
    }

    @Override
    public void renew() {
        if (isHeldByCurrentThread()) {
            doRenew(getLockEntry());
        }
    }

    /**
     * e.g. 5b978978-ed05-4715-b9b0-bc0217278329:78
     */
    public String getLockEntry() {
        return instanceId + ":" + Thread.currentThread().getId();
    }

    /**
     * 判断锁是否被当前线程持有
     */
    @Override
    public abstract boolean isHeldByCurrentThread();

    /**
     * 当锁被其他线程占用时，订阅锁的状态
     */
    protected abstract SubscribeLatch subscribe(String entry);

    /**
     * 取消锁的订阅
     */
    protected abstract void unSubscribe(String entry);

    /**
     * 申请锁，并返回锁的持续时间
     *
     * @return null, 获取锁成功； >= 0, 获取锁失败，返回锁的过期时间； < 0， 锁申请失败，但锁已过期。
     */
    protected abstract Long doLock(long durationMillis, String entry);

    /**
     * 续锁
     *
     * @return >= 0, 续锁成功，返回锁的过期时间； == 0， 续锁失败
     */
    protected abstract long doRenew(String entry);

    /**
     * 释放锁
     */
    protected abstract void doUnLock(String entry);
}
