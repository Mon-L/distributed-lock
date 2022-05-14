package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public class LockSubscriptionEntry implements SubscriptionListener {

    public static final Long UNLOCK_MESSAGE = 0L;

    private int count;
    private final String name;
    private final Semaphore semaphore = new Semaphore(0);
    private final CompletableFuture<LockSubscriptionEntry> subscriptionPromise;

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

    public CompletableFuture<LockSubscriptionEntry> getSubscriptionPromise() {
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
        return "lock subscription entry : " + name;
    }
}
