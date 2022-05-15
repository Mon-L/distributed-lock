package cn.zcn.distributed.lock;

import cn.zcn.distributed.lock.exception.LockException;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.subscription.LockSubscriptionEntry;
import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 分布式锁的结构
 * -lock-{lock name} = {
 * {UUID}:{thread id} = {lock count}
 * }
 */
public abstract class BaseZLock implements ZLock {

    private final static String LOCK_PREFIX = "-lock-";

    protected final String lockName;
    protected final String instanceId;

    private final Timer timer;
    private final LockSubscription lockSubscription;
    private final Config config;
    private Timeout renewTimeout;


    /**
     * @param lock       分布式锁的名称
     * @param instanceId UUID
     * @param timer      定时器
     */
    public BaseZLock(String lock, String instanceId, Timer timer, Config config, LockSubscription lockSubscription) {
        this.lockName = LOCK_PREFIX + lock;
        this.instanceId = instanceId;
        this.timer = timer;
        this.config = config;
        this.lockSubscription = lockSubscription;
    }

    private void timeout(CompletableFuture<?> future) {
        Timeout task = timer.newTimeout(t -> {
            future.completeExceptionally(new TimeoutException("Subscribe lock timeout."));
        }, config.getTimeout(), TimeUnit.MILLISECONDS);

        future.whenComplete((r, e) -> task.cancel());
    }

    @Override
    public void lock(long duration, TimeUnit durationTimeUnit) throws InterruptedException {
        String lockEntryName = getLockEntry();
        long durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, lockEntryName);
        if (ttl == null) {
            //获取锁成功
            return;
        }

        //获取锁失败，订阅锁的状态
        CompletableFuture<LockSubscriptionEntry> subPromise = lockSubscription.subscribe(lockName);
        LockSubscriptionEntry lockSubEntry;

        try {
            lockSubEntry = subPromise.get();
        } catch (InterruptedException e) {
            subPromise.completeExceptionally(e);
            throw e;
        } catch (ExecutionException e) {
            throw new LockException("Unexpected exception while subscribe lock.", e.getCause());
        }

        //监听是否订阅超时
        timeout(subPromise);

        try {
            while (true) {
                //尝试获取锁
                ttl = innerLock(durationMillis, lockEntryName);
                if (ttl == null) {
                    //获取锁成功
                    return;
                }

                if (ttl >= 0) {
                    //锁已被其他线程锁定，需等待 ttl 毫秒
                    lockSubEntry.getUnLockLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    // 获取锁失败，但锁已过期
                    lockSubEntry.getUnLockLatch().acquire();
                }
            }
        } finally {
            lockSubscription.unsubscribe(lockSubEntry, lockName);
        }
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit waitTimeUnit, long duration, TimeUnit durationTimeUnit) throws InterruptedException {
        String lockEntryName = getLockEntry();
        long startMillis = System.currentTimeMillis(),
                waitTimeMillis = waitTimeUnit.toMillis(waitTime),
                durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, lockEntryName);
        if (ttl == null) {
            //获取锁成功
            return true;
        }

        waitTimeMillis -= System.currentTimeMillis() - startMillis;
        if (waitTimeMillis <= 0) {
            return false;
        }

        //获取锁失败，订阅锁的状态
        CompletableFuture<LockSubscriptionEntry> subPromise = lockSubscription.subscribe(lockName);
        LockSubscriptionEntry lockSubEntry;

        try {
            lockSubEntry = subPromise.get(waitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            if (!subPromise.cancel(false)) {
                subPromise.whenComplete((r, t) -> {
                    if (t == null) {
                        lockSubscription.unsubscribe(r, lockName);
                    }
                });
            }
            return false;
        }

        try {
            while (true) {
                waitTimeMillis -= System.currentTimeMillis() - startMillis;
                if (waitTimeMillis <= 0) {
                    return false;
                }

                //尝试获取锁
                ttl = innerLock(durationMillis, lockEntryName);
                if (ttl == null) {
                    //获取锁成功
                    return true;
                }

                waitTimeMillis -= System.currentTimeMillis() - startMillis;
                if (waitTimeMillis <= 0) {
                    return false;
                }

                if (ttl >= 0 && ttl < waitTimeMillis) {
                    lockSubEntry.getUnLockLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    lockSubEntry.getUnLockLatch().tryAcquire(waitTimeMillis, TimeUnit.MILLISECONDS);
                }
            }
        } finally {
            lockSubscription.unsubscribe(lockSubEntry, lockName);
        }
    }

    private Long innerLock(long durationMillis, String lockEntryName) {
        Long ttl = doLock(durationMillis, lockEntryName);
        setRenewTimer(ttl, lockEntryName);
        return ttl;
    }

    private void setRenewTimer(long durationMillis, String lockEntryName) {
        this.renewTimeout = timer.newTimeout(t -> {
            if (t.isCancelled()) {
                return;
            }

            long ttl = doRenew(lockEntryName);
            if (ttl > 0) {
                setRenewTimer(ttl, lockEntryName);
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
    protected String getLockEntry() {
        return instanceId + ":" + Thread.currentThread().getId();
    }

    /**
     * 判断锁是否被当前线程持有
     */
    @Override
    public abstract boolean isHeldByCurrentThread();

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
