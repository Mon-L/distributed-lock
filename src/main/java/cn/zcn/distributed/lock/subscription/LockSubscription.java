package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class LockSubscription {

    private final LockSubscriptionService subscriptionService;
    private final ConcurrentMap<String, LockSubscriptionEntry> entries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SerialTaskQueen> queens = new ConcurrentHashMap<>();

    public LockSubscription(LockSubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    public CompletableFuture<LockSubscriptionEntry> subscribe(String channel) {
        CompletableFuture<LockSubscriptionEntry> newPromise = new CompletableFuture<>();

        SerialTaskQueen queen = queens.computeIfAbsent(channel, s -> new SerialTaskQueen());
        queen.add(() -> {
            LockSubscriptionEntry newSub = new LockSubscriptionEntry(channel, newPromise);
            newSub.increment();

            LockSubscriptionEntry oldSub = entries.putIfAbsent(channel, newSub);
            if (oldSub != null) {
                oldSub.increment();
                oldSub.getSubscriptionPromise().whenComplete((lockSubEntry, t) -> {
                    queen.runNext();
                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        return;
                    }

                    newPromise.complete(newSub);
                });
                return;
            }

            CompletableFuture<?> subFuture = subscriptionService.subscribe(channel, newSub);
            subFuture.whenComplete((BiConsumer<Object, Throwable>) (r, t) -> {
                if (t != null) {
                    newPromise.completeExceptionally(t);
                    return;
                }

                newPromise.complete(newSub);
                queen.runNext();
            });
        });

        return newPromise;
    }

    public void unsubscribe(LockSubscriptionEntry entry, String channel) {
        SerialTaskQueen serialTaskQueen = queens.get(channel);
        serialTaskQueen.add(() -> {
            if (entry.decrement() == 0) {
                subscriptionService.unsubscribe(channel)
                        .whenComplete((BiConsumer<Object, Throwable>) (o, t) -> serialTaskQueen.runNext());
            } else {
                serialTaskQueen.runNext();
            }
        });
    }
}
