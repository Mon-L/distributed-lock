package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.LockException;
import cn.zcn.distributed.lock.redis.subscription.LockSubscription;
import cn.zcn.distributed.lock.redis.subscription.LockSubscriptionHolder;
import io.netty.util.Timeout;
import io.netty.util.Timer;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 分布式锁骨架
 */
public abstract class AbstractRedisLock implements RedisLock {

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
    private static final String LOCK_PREFIX = "distributed-lock:";
    protected static final int DEFAULT_LOCK_DURATION = 20;

    private final Timer timer;
    protected final LockSubscription lockSubscription;

    /**
     * 分布式锁名
     */
    protected final String lockRawName;

    /**
     * {@code LOCK_PREFIX} + ":" + {@code lockRawName}
     * e.g. distributed-lock:account
     */
    protected final String lockEntryName;

    /**
     * 客户端 ID
     */
    protected final ClientId clientId;

    /**
     * @param lock     分布式锁的名称
     * @param clientId 客户端ID
     * @param timer    定时器
     */
    public AbstractRedisLock(String lock, ClientId clientId, Timer timer, LockSubscription lockSubscription) {
        this.lockRawName = lock;
        this.lockEntryName = withLockPrefix(lock);
        this.clientId = clientId;
        this.timer = timer;
        this.lockSubscription = lockSubscription;
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

    protected String withLockPrefix(String value) {
        return LOCK_PREFIX + value;
    }

    /**
     * 清除续锁的定时任务
     *
     * @param force ture, 强制清除定时任务; false, 如果当前没有任何线程还持有锁, 则清除定时任务。否则，不清除。
     */
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
        boolean success = doUnLock(Thread.currentThread().getId());
        if (success) {
            //清除续锁的定时任务
            clearRenewSchedule(false);
        }
    }

    protected String getLockHolderEntry(long threadId) {
        return clientId.getValue() + ":" + threadId;
    }

    @Override
    public abstract boolean isHeldByCurrentThread();

    /**
     * 请求锁
     *
     * @param durationMillis 锁的持续时间
     * @param threadId       当前线程ID
     * @return null, 请求锁成功; >= 0, 获取锁失败, 返回当前锁的过期时间; < 0, 请求锁失败。
     */
    protected abstract Long doLock(long durationMillis, long threadId);

    /**
     * 给分布式锁续期
     *
     * @param durationMillis 续期时长
     * @param threadId       当前线程ID
     * @return true, 续锁成功; false, 续锁失败
     */
    protected abstract boolean doRenew(long durationMillis, long threadId);

    /**
     * 释放锁
     *
     * @param threadId 当前线程ID
     * @return true, 释放锁成功; false,释放锁失败
     */
    protected abstract boolean doUnLock(long threadId);
}
