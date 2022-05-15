package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockSubscription {

    public static final Long UNLOCK_MESSAGE = 0L;

    private final LockSubscriptionService subscriptionService;
    private final ConcurrentMap<String, LockSubscriptionEntry> entries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncRunnableQueen> queens = new ConcurrentHashMap<>();

    public LockSubscription(LockSubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    public CompletableFuture<LockSubscriptionEntry> subscribe(String channel) {
        CompletableFuture<LockSubscriptionEntry> newPromise = new CompletableFuture<>();

        AsyncRunnableQueen queen = queens.computeIfAbsent(channel, s -> new AsyncRunnableQueen());
        queen.add(() -> {
            if (newPromise.isDone()) {
                return;
            }

            LockSubscriptionEntry entry = new LockSubscriptionEntry(channel, newPromise);
            entry.increment();

            LockSubscriptionEntry oldSubEntry = entries.putIfAbsent(channel, entry);
            if (oldSubEntry != null) {
                oldSubEntry.increment();
                oldSubEntry.getResult().whenComplete((lockSubEntry, t) -> {
                    if (t == null) {
                        newPromise.complete(entry);
                    } else {
                        newPromise.completeExceptionally(t);
                    }
                });
                return;
            }

            SubscriptionListener listener = createListener(channel, entry);
            CompletableFuture<?> subscriptionPromise = subscriptionService.subscribe(channel, listener, newPromise);

            newPromise.whenComplete((r, e) -> {
                if (e != null) {
                    subscriptionPromise.completeExceptionally(e);
                }
            });

            subscriptionPromise.whenComplete((r, t) -> {
                if (t == null) {
                    entry.getResult().complete(entry);
                } else {
                    entry.getResult().completeExceptionally(t);
                }
            });
        });

        newPromise.whenComplete((r, e) -> queen.runNext());

        return newPromise;
    }

    public void unsubscribe(LockSubscriptionEntry entry, String channel) {
        AsyncRunnableQueen queen = queens.get(channel);
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

            if (message.equals(UNLOCK_MESSAGE)) {
                lockSubscriptionEntry.getUnLockLatch().release();
            }
        };
    }
}
