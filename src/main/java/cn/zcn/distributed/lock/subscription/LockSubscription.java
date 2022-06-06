package cn.zcn.distributed.lock.subscription;

import cn.zcn.distributed.lock.LongEncoder;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockSubscription {

    public static final byte[] UNLOCK_MESSAGE = LongEncoder.encode(0L);

    private final LockSubscriptionService subscriptionService;
    private final ConcurrentMap<String, LockSubscriptionHolder> holders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SerialRunnableQueen> queens = new ConcurrentHashMap<>();

    public LockSubscription(LockSubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    public CompletableFuture<LockSubscriptionHolder> subscribe(String channel) {
        CompletableFuture<LockSubscriptionHolder> newPromise = new CompletableFuture<>();

        SerialRunnableQueen queen = queens.computeIfAbsent(channel, s -> new SerialRunnableQueen());
        queen.add(() -> {
            if (newPromise.isDone()) {
                //订阅被打断或超时
                queen.runNext();
                return;
            }

            LockSubscriptionHolder newHolder = new LockSubscriptionHolder(channel);
            newHolder.increment();

            LockSubscriptionHolder oldHolder = holders.putIfAbsent(channel, newHolder);
            if (oldHolder != null) {
                oldHolder.increment();
                queen.runNext();
                oldHolder.getPromise().whenComplete((r, t) -> {
                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        unsubscribe(oldHolder, channel);
                    } else {
                        newPromise.complete(r);
                    }
                });
                return;
            }

            LockMessageListener listener = createListener(channel, newHolder);
            CompletableFuture<?> subscriptionPromise = subscriptionService.subscribe(channel, listener);
            subscriptionPromise.whenComplete((r, t) -> newHolder.complete(t));

            newHolder.getPromise().whenComplete((r, t) -> {
                if (newPromise.isDone()) {
                    //订阅被打断或超时
                    unsubscribe(newHolder, channel);
                } else {
                    if (t != null) {
                        newPromise.completeExceptionally(t);
                        unsubscribe(newHolder, channel);
                    } else {
                        newPromise.complete(r);
                    }
                }
                queen.runNext();
            });
        });

        return newPromise;
    }

    public void unsubscribe(LockSubscriptionHolder entry, String channel) {
        SerialRunnableQueen queen = queens.get(channel);
        queen.add(() -> {
            if (entry.decrement() == 0) {
                holders.remove(channel);
                subscriptionService.unsubscribe(channel)
                        .whenComplete((r, t) -> queen.runNext());
            } else {
                queen.runNext();
            }
        });
    }

    private LockMessageListener createListener(String channelName, LockSubscriptionHolder lockSubscriptionHolder) {
        return (channel, message) -> {
            if (!channelName.equals(channel)) {
                return;
            }

            if (message instanceof byte[]) {
                if (Arrays.equals((byte[]) message, UNLOCK_MESSAGE)) {
                    lockSubscriptionHolder.getUnLockLatch().release();
                }
            }
        };
    }
}
