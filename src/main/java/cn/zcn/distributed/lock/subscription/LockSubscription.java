package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockSubscription {

    public static final Long UNLOCK_MESSAGE = 0L;

    private final LockSubscriptionService subscriptionService;
    private final ConcurrentMap<String, LockSubscriptionEntry> entries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SerialRunnableQueen> queens = new ConcurrentHashMap<>();

    public LockSubscription(LockSubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    public CompletableFuture<LockSubscriptionEntry> subscribe(String channel) {
        CompletableFuture<LockSubscriptionEntry> newPromise = new CompletableFuture<>();

        SerialRunnableQueen queen = queens.computeIfAbsent(channel, s -> new SerialRunnableQueen());
        queen.add(() -> {
            if (newPromise.isDone()) {
                //订阅被打断或超时
                queen.runNext();
                return;
            }

            LockSubscriptionEntry newEntry = new LockSubscriptionEntry(channel);
            newEntry.increment();

            LockSubscriptionEntry oldEntry = entries.putIfAbsent(channel, newEntry);
            if (oldEntry != null) {
                oldEntry.increment();
                queen.runNext();
                oldEntry.getPromise().whenComplete((r, t) -> {
                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        unsubscribe(oldEntry, channel);
                    } else {
                        newPromise.complete(r);
                    }
                });
                return;
            }

            SubscriptionListener listener = createListener(channel, newEntry);
            CompletableFuture<?> subscriptionPromise = subscriptionService.subscribe(channel, listener);
            subscriptionPromise.whenComplete((r, t) -> newEntry.complete(t));

            newEntry.getPromise().whenComplete((r, t) -> {
                if (newPromise.isDone()) {
                    //订阅被打断或超时
                    unsubscribe(newEntry, channel);
                } else {
                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        unsubscribe(newEntry, channel);
                    } else {
                        newPromise.complete(r);
                    }
                }
                queen.runNext();
            });
        });

        return newPromise;
    }

    public void unsubscribe(LockSubscriptionEntry entry, String channel) {
        SerialRunnableQueen queen = queens.get(channel);
        queen.add(() -> {
            if (entry.decrement() == 0) {
                entries.remove(channel);
                subscriptionService.unsubscribe(channel)
                        .whenComplete((r, t) -> queen.runNext());
            } else {
                queen.runNext();
            }
        });
    }

    private SubscriptionListener createListener(String channelName, LockSubscriptionEntry lockSubscriptionEntry) {
        return (channel, message) -> {
            if (!channelName.equals(channel)) {
                return;
            }

            if (UNLOCK_MESSAGE.equals(message)) {
                lockSubscriptionEntry.getUnLockLatch().release();
            }
        };
    }
}
