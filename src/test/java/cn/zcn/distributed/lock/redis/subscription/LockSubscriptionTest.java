package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.subscription.LockSubscriptionEntry;
import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class LockSubscriptionTest {

    private LockSubscription subscription;
    private LockSubscriptionService subscriptionService;

    @Before
    public void before() {
        subscriptionService = Mockito.mock(LockSubscriptionService.class);
        subscription = new LockSubscription(subscriptionService);
    }

    @Test
    public void testSubscribeWhenSerial() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Void> subscriptionFuture = new CompletableFuture<>();
        Mockito.when(subscriptionService.subscribe(Mockito.any(), Mockito.any())).thenReturn(subscriptionFuture);
        new Thread(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            subscriptionFuture.complete(null);
        }).start();

        subscription.subscribe("1").get(300, TimeUnit.MILLISECONDS);
        subscription.subscribe("1").get(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSubscribeWhenConcurrent() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Void> subscriptionFuture = new CompletableFuture<>();
        Mockito.when(subscriptionService.subscribe(Mockito.any(), Mockito.any())).thenReturn(subscriptionFuture);
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            subscriptionFuture.complete(null);
        }).start();

        int size = 20;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(size);
        List<CompletableFuture<LockSubscriptionEntry>> futures = Collections.synchronizedList(new ArrayList<>(20));
        for (int i = 0; i < size; i++) {
            new Thread(() -> {
                try {
                    cyclicBarrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

                futures.add(subscription.subscribe("1"));
            }).start();
        }

        Thread.sleep(3000);

        for (CompletableFuture<LockSubscriptionEntry> f : futures) {
            Assert.assertTrue(f.isDone());
        }
    }

    @Test
    public void testSubscribeThrowException() throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Void> subscriptionFuture = new CompletableFuture<>();
        Mockito.when(subscriptionService.subscribe(Mockito.any(), Mockito.any())).thenReturn(subscriptionFuture);
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            subscriptionFuture.completeExceptionally(new RuntimeException("fff"));
        }).start();

        int size = 20;
        CyclicBarrier cyclicBarrier = new CyclicBarrier(size);
        List<CompletableFuture<LockSubscriptionEntry>> futures = Collections.synchronizedList(new ArrayList<>(20));
        for (int i = 0; i < size; i++) {
            new Thread(() -> {
                try {
                    cyclicBarrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }

                futures.add(subscription.subscribe("1"));
            }).start();
        }

        Thread.sleep(3000);

        for (CompletableFuture<LockSubscriptionEntry> f : futures) {
            Assert.assertTrue(f.isCompletedExceptionally());
        }
    }
}
