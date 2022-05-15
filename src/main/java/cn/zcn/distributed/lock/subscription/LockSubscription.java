package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class LockSubscription {

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
            LockSubscriptionEntry newSubEntry = new LockSubscriptionEntry(channel, newPromise);
            newSubEntry.increment();

            LockSubscriptionEntry oldSubEntry = entries.putIfAbsent(channel, newSubEntry);
            if (oldSubEntry != null) {
                oldSubEntry.increment();
                oldSubEntry.getResult().whenComplete((lockSubEntry, t) -> {
                    queen.runNext();
                    if (t == null) {
                        newPromise.complete(newSubEntry);
                    } else {
                        newPromise.completeExceptionally(t);
                    }
                });
                return;
            }

            CompletableFuture<?> subFuture = subscriptionService.subscribe(channel, newSubEntry);
            subFuture.whenComplete((BiConsumer<Object, Throwable>) (r, t) -> {
                if (t == null) {
                    newPromise.complete(newSubEntry);
                } else {
                    newPromise.completeExceptionally(t);
                }
                queen.runNext();
            });
        });

        return newPromise;
    }

    public void unsubscribe(LockSubscriptionEntry entry, String channel) {
        AsyncRunnableQueen asyncRunnableQueen = queens.get(channel);
        asyncRunnableQueen.add(() -> {
            if (entry.decrement() == 0) {
                entries.remove(channel);
                subscriptionService.unsubscribe(channel)
                        .whenComplete((BiConsumer<Object, Throwable>) (o, t) -> asyncRunnableQueen.runNext());
            } else {
                asyncRunnableQueen.runNext();
            }
        });
    }
}
