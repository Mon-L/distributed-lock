package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public class LockSubscriptionEntry implements SubscriptionListener {

    public static final Long UNLOCK_MESSAGE = 0L;

    /**
     * 锁的订阅数量
     */
    private int count;

    /**
     * 锁的名称
     */
    private final String name;

    /**
     * 订阅结果
     */
    private final CompletableFuture<LockSubscriptionEntry> subscriptionPromise;

    private final Semaphore semaphore = new Semaphore(0);

    public LockSubscriptionEntry(String name, CompletableFuture<LockSubscriptionEntry> subscriptionPromise) {
        this.name = name;
        this.subscriptionPromise = subscriptionPromise;
    }

    public void increment() {
        count++;
    }

    public int decrement() {
        return count--;
    }

    public CompletableFuture<LockSubscriptionEntry> getResult() {
        return subscriptionPromise;
    }

    public Semaphore getLatch() {
        return semaphore;
    }

    @Override
    public void onMessage(String channel, Object message) {
        if (!name.equals(channel)) {
            return;
        }

        if (message.equals(UNLOCK_MESSAGE)) {
            semaphore.release();
        }
    }

    @Override
    public String toString() {
        return "LockSubscriptionEntry{" +
                "name=" + name +
                ", count='" + count + '\'' +
                '}';
    }
}
