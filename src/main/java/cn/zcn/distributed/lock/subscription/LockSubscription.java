package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 用于管理分布式锁的订阅、取消订阅、通知。
 * 多个线程同一时刻只需要订阅每个锁一次，当没有线程需要订阅锁的状态时取消订阅。
 */
public class LockSubscription {

    private final LockSubscriptionService subscriptionService;

    /**
     * 存储每个锁的订阅，每个锁同一时刻只需要订阅一次
     */
    private final ConcurrentMap<String, LockSubscriptionHolder> holders = new ConcurrentHashMap<>();

    /**
     * 订阅任务队列，同一个锁使用同一个队列。串行化执行锁的订阅与取消订阅任务。
     */
    private final ConcurrentMap<String, SerialRunnableQueen> queens = new ConcurrentHashMap<>();

    public LockSubscription(LockSubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    /**
     * 串行执行同一个锁的订阅、取消订阅事件，每个锁同一时刻只需要订阅一次。
     * 如果未订阅该锁则执行订阅任务，并返回订阅结果。
     * 如果已经订阅了该锁，返回之前的订阅结果。
     *
     * @param channel 锁
     * @return 订阅状态
     */
    public CompletableFuture<LockSubscriptionHolder> subscribe(String channel) {
        CompletableFuture<LockSubscriptionHolder> newPromise = new CompletableFuture<>();

        SerialRunnableQueen queen = queens.computeIfAbsent(channel, s -> new SerialRunnableQueen());
        queen.add(() -> {
            if (newPromise.isDone()) {
                queen.runNext();
                return;
            }

            LockSubscriptionHolder newHolder = new LockSubscriptionHolder(channel);
            newHolder.increment();

            LockSubscriptionHolder oldHolder = holders.putIfAbsent(channel, newHolder);
            if (oldHolder != null) {    //已经订阅过该锁
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

            //未订阅该锁。创建订阅监听并订阅锁的状态
            LockStatusListener listener = createListener(channel, newHolder);
            CompletableFuture<?> subscriptionPromise = subscriptionService.subscribe(channel, listener);
            subscriptionPromise.whenComplete((r, t) -> newHolder.complete(t));

            newHolder.getPromise().whenComplete((r, t) -> {
                if (newPromise.isDone()) {
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

    /**
     * 取消锁的订阅。
     * 当没有任何线程需要订阅该锁时，才需要发送取消订阅命令。
     *
     * @param entry   锁的订阅状态
     * @param channel 锁
     */
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

    private LockStatusListener createListener(String channelName, LockSubscriptionHolder lockSubscriptionHolder) {
        return (channel, message) -> {
            if (!channelName.equals(channel)) {
                return;
            }

            lockSubscriptionHolder.getUnLockLatch().release();
        };
    }
}
