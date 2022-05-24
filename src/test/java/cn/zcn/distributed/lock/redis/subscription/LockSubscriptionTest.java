package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.subscription.LockSubscriptionEntry;
import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class LockSubscriptionTest {

    private final String lockName = "lock1";
    private LockSubscription lockSubscription;
    private CompletableFuture<Void> subscriptionPromise;
    private CompletableFuture<Void> unsubscriptionPromise;
    private LockSubscriptionService lockSubscriptionService;

    @BeforeEach
    public void before() {
        subscriptionPromise = new CompletableFuture<>();
        unsubscriptionPromise = new CompletableFuture<>();
        lockSubscriptionService = Mockito.mock(LockSubscriptionService.class);
        lockSubscription = Mockito.spy(new LockSubscription(lockSubscriptionService));
    }

    @Test
    public void testSubscribeWhenSuccess() throws ExecutionException, InterruptedException {
        Mockito.when(lockSubscriptionService.subscribe(Mockito.any(), Mockito.any())).thenReturn(subscriptionPromise);
        subscriptionPromise.complete(null);

        CompletableFuture<LockSubscriptionEntry> promise = lockSubscription.subscribe(lockName);
        LockSubscriptionEntry entry = promise.get();

        Assertions.assertTrue(entry.getPromise().isDone());
        Assertions.assertEquals(1, entry.getCount());
    }

    @Test
    public void concurrentTestSubscribeWhenSuccess() {
        Mockito.when(lockSubscriptionService.subscribe(Mockito.any(), Mockito.any())).thenReturn(subscriptionPromise);
        Mockito.when(lockSubscriptionService.unsubscribe(Mockito.anyString())).thenReturn(unsubscriptionPromise);

        int threadNum = 5, iterations = 20;
        CountDownLatch latch = new CountDownLatch(threadNum * 20);
        List<CompletableFuture<LockSubscriptionEntry>> promises = Collections.synchronizedList(new ArrayList<>());
        asyncRun(threadNum, () -> {
            for (int j = 0; j < iterations; j++) {
                promises.add(lockSubscription.subscribe(lockName));
                latch.countDown();
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //订阅 channel 成功
        subscriptionPromise.complete(null);

        LockSubscriptionEntry entry;
        try {
            entry = promises.get(0).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        //判断 channel 订阅次数
        Assertions.assertEquals(threadNum * iterations, entry.getCount());
        Mockito.verify(lockSubscriptionService, Mockito.times(1)).subscribe(Mockito.anyString(), Mockito.any());

        promises.forEach(f -> {
            Assertions.assertTrue(f.isDone());
            Assertions.assertFalse(f.isCompletedExceptionally());
        });
    }

    @Test
    public void concurrentTestSubscribeWhenFirstInterrupted() {
        Mockito.when(lockSubscriptionService.subscribe(Mockito.any(), Mockito.any())).thenReturn(subscriptionPromise);
        Mockito.when(lockSubscriptionService.unsubscribe(Mockito.anyString())).thenReturn(unsubscriptionPromise);

        CompletableFuture<LockSubscriptionEntry> firstPromise = lockSubscription.subscribe(lockName);

        int threadNum = 5, iterations = 20;
        CountDownLatch latch = new CountDownLatch(threadNum * 20);
        List<CompletableFuture<LockSubscriptionEntry>> promises = Collections.synchronizedList(new ArrayList<>());
        asyncRun(threadNum, () -> {
            for (int j = 0; j < iterations; j++) {
                promises.add(lockSubscription.subscribe(lockName));
                latch.countDown();
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        //第一个订阅被打断
        firstPromise.completeExceptionally(new InterruptedException("Interrupted"));

        //随后订阅 channel 成功
        subscriptionPromise.complete(null);

        LockSubscriptionEntry entry;
        try {
            entry = promises.get(0).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        //判断 channel 订阅次数
        Assertions.assertEquals(threadNum * iterations, entry.getCount());
        Mockito.verify(lockSubscriptionService, Mockito.times(1)).subscribe(Mockito.anyString(), Mockito.any());

        //剩下的订阅都成功
        promises.forEach(f -> {
            Assertions.assertTrue(f.isDone());
            Assertions.assertFalse(f.isCompletedExceptionally());
        });

        //取消所有订阅
        promises.forEach(future -> lockSubscription.unsubscribe(entry, lockName));

        //取消订阅成功
        unsubscriptionPromise.complete(null);

        //只取消订阅 channel 一次
        Mockito.verify(lockSubscriptionService, Mockito.times(1)).unsubscribe(Mockito.any());
        Assertions.assertEquals(0, entry.getCount());
    }

    private void asyncRun(int threadNum, Runnable runnable) {
        for (int i = 0; i < threadNum; i++) {
            new Thread(runnable).start();
        }
    }
}
