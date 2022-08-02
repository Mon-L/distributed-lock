package cn.zcn.distributed.lock.redis.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class LockSubscriptionHolder {

    /**
     * 锁的订阅数量
     */
    private final AtomicInteger count = new AtomicInteger(0);

    /**
     * 锁的订阅结果
     */
    private final CompletableFuture<LockSubscriptionHolder> promise = new CompletableFuture<>();

    /**
     * 锁的名称
     */
    private final String name;

    private final Semaphore unLockSemaphore = new Semaphore(0);

    public LockSubscriptionHolder(String name) {
        this.name = name;
    }

    public void increment() {
        count.incrementAndGet();
    }

    public long decrement() {
        return count.decrementAndGet();
    }

    public void complete(Throwable t) {
        if (t != null) {
            promise.completeExceptionally(t);
        } else {
            promise.complete(this);
        }
    }

    public CompletableFuture<LockSubscriptionHolder> getPromise() {
        return promise;
    }

    public Semaphore getUnLockLatch() {
        return unLockSemaphore;
    }

    public int getCount() {
        return count.get();
    }

    @Override
    public String toString() {
        return "LockSubscriptionHolder{" +
                "name=" + name +
                ", count='" + count + '\'' +
                '}';
    }
}
