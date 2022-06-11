package cn.zcn.distributed.lock;

import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.subscription.LockSubscriptionHolder;
import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 分布式锁的结构
 * -lock-{lock name} = {
 * {UUID}:{thread id} = {lock count}
 * }
 */
public abstract class AbstractLock implements Lock {

    private static class RenewLockHolder {
        private final AtomicLong count = new AtomicLong(0);
        private volatile Timeout timeout;

        private void increase() {
            count.incrementAndGet();
        }

        private long decrease() {
            return count.decrementAndGet();
        }

        private void setTimeout(Timeout timeout) {
            this.timeout = timeout;
        }

        private Timeout getTimeout() {
            return timeout;
        }
    }

    private static final Map<String, RenewLockHolder> lockRenewHolders = new ConcurrentHashMap<>();
    protected static final String LOCK_PREFIX = "distributed-lock:";
    protected static final int DEFAULT_LOCK_DURATION = 20;

    private final Timer timer;
    protected final LockSubscription lockSubscription;
    protected final String lockName;
    protected final String lockEntryName;
    protected final String clientId;

    /**
     * @param lock     分布式锁的名称
     * @param clientId UUID
     * @param timer    定时器
     */
    public AbstractLock(String lock, String clientId, Timer timer, LockSubscription lockSubscription) {
        this.lockName = lock;
        this.lockEntryName = LOCK_PREFIX + lock;
        this.clientId = clientId;
        this.timer = timer;
        this.lockSubscription = lockSubscription;
    }

    private void startTimeoutSchedule(CompletableFuture<?> promise) {
        Timeout task = timer.newTimeout(t -> {
            promise.completeExceptionally(new TimeoutException("Subscribe lock timeout."));
        }, 3, TimeUnit.SECONDS);

        promise.whenComplete((r, e) -> task.cancel());
    }

    @Override
    public void lock() throws InterruptedException {
        lock(DEFAULT_LOCK_DURATION, TimeUnit.SECONDS);
    }

    @Override
    public void lock(long duration, TimeUnit durationTimeUnit) throws InterruptedException {
        long threadId = Thread.currentThread().getId();
        long durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, threadId);
        if (ttl == null) {
            //获取锁成功
            return;
        }

        //获取锁失败，订阅锁的状态
        CompletableFuture<LockSubscriptionHolder> subscriptionPromise = subscribe(threadId);
        LockSubscriptionHolder lockSubscriptionHolder;

        try {
            lockSubscriptionHolder = subscriptionPromise.get();
        } catch (InterruptedException e) {
            subscriptionPromise.completeExceptionally(e);
            throw e;
        } catch (ExecutionException e) {
            throw new LockException("Unexpected exception while subscribe lock.", e.getCause());
        }

        //监听是否订阅超时
        startTimeoutSchedule(subscriptionPromise);

        try {
            while (true) {
                //尝试获取锁
                ttl = innerLock(durationMillis, threadId);
                if (ttl == null) {
                    //获取锁成功
                    return;
                }

                if (ttl >= 0) {
                    //锁已被其他线程锁定，需等待 ttl 毫秒
                    lockSubscriptionHolder.getUnLockLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    // 获取锁失败，但锁已过期
                    lockSubscriptionHolder.getUnLockLatch().acquire();
                }
            }
        } finally {
            unsubscribe(lockSubscriptionHolder, threadId);
        }
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit waitTimeUnit, long duration, TimeUnit durationTimeUnit) throws InterruptedException {
        long threadId = Thread.currentThread().getId(),
                startMillis = System.currentTimeMillis(),
                waitTimeMillis = waitTimeUnit.toMillis(waitTime),
                durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, threadId);
        if (ttl == null) {
            //获取锁成功
            return true;
        }

        waitTimeMillis -= System.currentTimeMillis() - startMillis;
        if (waitTimeMillis <= 0) {
            return false;
        }

        //获取锁失败，订阅锁的状态
        CompletableFuture<LockSubscriptionHolder> subPromise = subscribe(threadId);
        LockSubscriptionHolder lockSubscriptionHolder;

        try {
            lockSubscriptionHolder = subPromise.get(waitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            if (!subPromise.cancel(false)) {
                subPromise.whenComplete((r, t) -> {
                    if (t == null) {
                        unsubscribe(r, threadId);
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
                ttl = innerLock(durationMillis, threadId);
                if (ttl == null) {
                    //获取锁成功
                    return true;
                }

                waitTimeMillis -= System.currentTimeMillis() - startMillis;
                if (waitTimeMillis <= 0) {
                    return false;
                }

                if (ttl >= 0 && ttl < waitTimeMillis) {
                    lockSubscriptionHolder.getUnLockLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    lockSubscriptionHolder.getUnLockLatch().tryAcquire(waitTimeMillis, TimeUnit.MILLISECONDS);
                }
            }
        } finally {
            unsubscribe(lockSubscriptionHolder, threadId);
        }
    }

    private Long innerLock(long durationMillis, long threadId) {
        Long ttl = doLock(durationMillis, threadId);
        if (ttl == null) {
            startRenewScheduleIfNeeded(durationMillis, threadId);
        }
        return ttl;
    }

    private void startRenewScheduleIfNeeded(long durationMillis, long threadId) {
        RenewLockHolder renewLockHolder = new RenewLockHolder();
        RenewLockHolder oldEntry = lockRenewHolders.putIfAbsent(lockEntryName, renewLockHolder);

        if (oldEntry != null) {
            oldEntry.increase();
        } else {
            renewLockHolder.increase();
            doRenewSchedule(renewLockHolder, durationMillis, threadId);
        }
    }

    private void doRenewSchedule(RenewLockHolder e, long durationMillis, long threadId) {
        Timeout timeout = timer.newTimeout(taskTimeout -> {
            boolean ret = doRenew(durationMillis, threadId);
            if (ret) {
                doRenewSchedule(e, durationMillis, threadId);
            } else {
                clearRenewSchedule(true);
            }
        }, durationMillis / 3, TimeUnit.MILLISECONDS);

        e.setTimeout(timeout);
    }

    private void clearRenewSchedule(boolean force) {
        RenewLockHolder entry = lockRenewHolders.get(lockEntryName);
        if (entry != null) {
            long count = entry.decrease();

            if (force || count == 0) {
                lockRenewHolders.remove(lockEntryName);
                Timeout timeout = entry.getTimeout();
                if (!timeout.isCancelled()) {
                    timeout.cancel();
                }
            }
        }
    }

    protected CompletableFuture<LockSubscriptionHolder> subscribe(long threadId) {
        return lockSubscription.subscribe(lockEntryName);
    }

    protected void unsubscribe(LockSubscriptionHolder lockSubscriptionHolder, long threadId) {
        lockSubscription.unsubscribe(lockSubscriptionHolder, lockEntryName);
    }

    @Override
    public void unlock() {
        boolean ret = doUnLock(Thread.currentThread().getId());
        if (ret) {
            clearRenewSchedule(false);
        }
    }

    /**
     * e.g. 5b978978ed054715b9b0bc0217278329:78
     */
    protected String getLockEntry(long threadId) {
        return clientId + ":" + threadId;
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
    protected abstract Long doLock(long durationMillis, long threadId);

    /**
     * 续锁
     *
     * @return true, 续锁成功； false， 续锁失败
     */
    protected abstract boolean doRenew(long durationMillis, long threadId);

    /**
     * 释放锁
     */
    protected abstract boolean doUnLock(long threadId);
}
