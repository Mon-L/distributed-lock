package cn.zcn.distributed.lock;

import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.subscription.LockSubscriptionEntry;
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

    private static class RenewEntry {
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

    private static final Map<String, RenewEntry> renewEntries = new ConcurrentHashMap<>();
    private static final String LOCK_PREFIX = "-lock-";

    private final Timer timer;
    private final LockSubscription lockSubscription;
    private final Config config;

    protected final String lockName;
    protected final String instanceId;

    /**
     * @param lock       分布式锁的名称
     * @param instanceId UUID
     * @param timer      定时器
     */
    public AbstractLock(String lock, String instanceId, Timer timer, Config config, LockSubscription lockSubscription) {
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
        long threadId = Thread.currentThread().getId();
        long durationMillis = durationTimeUnit.toMillis(duration);

        Long ttl = innerLock(durationMillis, threadId);
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
                ttl = innerLock(durationMillis, threadId);
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
                    lockSubEntry.getUnLockLatch().tryAcquire(ttl, TimeUnit.MILLISECONDS);
                } else {
                    lockSubEntry.getUnLockLatch().tryAcquire(waitTimeMillis, TimeUnit.MILLISECONDS);
                }
            }
        } finally {
            lockSubscription.unsubscribe(lockSubEntry, lockName);
        }
    }

    private Long innerLock(long durationMillis, long threadId) {
        Long ttl = doLock(durationMillis, threadId);
        if (ttl == null) {
            startRenewTimer(durationMillis, threadId);
        }
        return ttl;
    }

    private void startRenewTimer(long durationMillis, long threadId) {
        RenewEntry renewEntry = new RenewEntry();
        RenewEntry oldEntry = renewEntries.putIfAbsent(lockName, renewEntry);

        if (oldEntry != null) {
            oldEntry.increase();
        } else {
            renewEntry.increase();
            setTimeout(renewEntry, durationMillis, threadId);
        }
    }

    private void setTimeout(RenewEntry e, long durationMillis, long threadId) {
        Timeout timeout = timer.newTimeout(taskTimeout -> {
            boolean ret = doRenew(durationMillis, threadId);
            if (ret) {
                setTimeout(e, durationMillis, threadId);
            } else {
                clearTimeout(true);
            }
        }, durationMillis / 3, TimeUnit.MILLISECONDS);

        e.setTimeout(timeout);
    }

    private void clearTimeout(boolean force) {
        RenewEntry entry = renewEntries.get(lockName);
        if (entry != null) {
            long count = entry.decrease();

            if (force || count == 0) {
                renewEntries.remove(lockName);
                Timeout timeout = entry.getTimeout();
                if (!timeout.isCancelled()) {
                    timeout.cancel();
                }
            }
        }
    }

    @Override
    public void unlock() {
        boolean ret = doUnLock(Thread.currentThread().getId());
        if (ret) {
            clearTimeout(false);
        }
    }

    /**
     * e.g. 5b978978ed054715b9b0bc0217278329:78
     */
    protected String getLockEntry(long threadId) {
        return instanceId + ":" + threadId;
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
