package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public class LockSubscriptionEntry {

    /**
     * 锁的订阅数量
     */
    private volatile int count;

    /**
     * 锁的名称
     */
    private final String name;

    /**
     * 订阅结果
     */
    private final CompletableFuture<LockSubscriptionEntry> promise;

    private final Semaphore unLockSemaphore = new Semaphore(0);

    public LockSubscriptionEntry(String name) {
        this.name = name;
        this.promise = new CompletableFuture<>();
    }

    public void increment() {
        ++count;
    }

    public int decrement() {
        return --count;
    }

    public void complete(Throwable t) {
        if (t != null) {
            promise.completeExceptionally(t);
        } else {
            promise.complete(this);
        }
    }

    public CompletableFuture<LockSubscriptionEntry> getPromise() {
        return promise;
    }

    public Semaphore getUnLockLatch() {
        return unLockSemaphore;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "LockSubscriptionEntry{" +
                "name=" + name +
                ", count='" + count + '\'' +
                '}';
    }
}
