package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.redis.subscription.LockSubscription;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionService;
import cn.zcn.distributed.lock.test.redis.RedisIntegrationTestContainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisFairLockIntegrationTest {

    private RedisFairLockImpl redisLock;

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisIntegrationTestContainer.getJedisPoolCommandFactory(), RedisIntegrationTestContainer.getJedisSubscriptionService()),
                Arguments.of(RedisIntegrationTestContainer.getLettuceCommandFactory(), RedisIntegrationTestContainer.getLettuceSubscriptionService())
        );
    }

    static Stream<Arguments> testLettuceParams() {
        return Stream.of(
                Arguments.of(RedisIntegrationTestContainer.getLettuceCommandFactory(), RedisIntegrationTestContainer.getLettuceSubscriptionService())
        );
    }

    private void initLock(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) {
        redisLock = new RedisFairLockImpl(UUID.randomUUID().toString(), ClientId.create(), RedisIntegrationTestContainer.getTimer(), new LockSubscription(subscriptionService), commandFactory);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testLock(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        long startTime = System.currentTimeMillis();
        redisLock.lock(3, TimeUnit.SECONDS);

        assertThat(System.currentTimeMillis() - startTime).isLessThan(400);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUnLock(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        redisLock.lock(10, TimeUnit.SECONDS);
        assertThat(redisLock.isHeldByCurrentThread()).isTrue();

        redisLock.unlock();
        assertThat(redisLock.isHeldByCurrentThread()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUnLockByAnotherThread(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);
        Semaphore semaphore = new Semaphore(0);

        redisLock.lock(10, TimeUnit.SECONDS);

        new Thread(() -> {
            //can't unlock by another thread.
            redisLock.unlock();
            semaphore.release();
        }).start();

        semaphore.acquire();
        assertThat(redisLock.isHeldByCurrentThread()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testHeldByCurrentThread(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        assertThat(redisLock.isHeldByCurrentThread()).isFalse();

        redisLock.lock(3, TimeUnit.SECONDS);
        assertThat(redisLock.isHeldByCurrentThread()).isTrue();

        redisLock.unlock();
        assertThat(redisLock.isHeldByCurrentThread()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testConcurrentLock(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        UnsafeCounter counter = new UnsafeCounter();

        CountDownLatch latch = new CountDownLatch(20);
        for (int i = 0; i < 20; i++) {
            new Thread(() -> {
                try {
                    redisLock.lock(3, TimeUnit.SECONDS);
                    counter.increment();
                    redisLock.unlock();

                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }

        latch.await(20, TimeUnit.SECONDS);

        assertThat(counter.count).isEqualTo(20);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testTryLockWait(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        Semaphore semaphore = new Semaphore(0);

        new Thread(() -> {
            try {
                redisLock.lock(3, TimeUnit.SECONDS);

                semaphore.release();
                TimeUnit.MILLISECONDS.sleep(2900);

                redisLock.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        semaphore.tryAcquire(5, TimeUnit.SECONDS);

        long startTime = System.currentTimeMillis();
        boolean locked = redisLock.tryLock(4, TimeUnit.SECONDS, 3, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        assertThat(locked).isTrue();
        assertThat(endTime - startTime).isBetween(2900L, 3100L);
    }

    @ParameterizedTest
    @MethodSource("testLettuceParams")
    void testFariLockOrdering(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        final ConcurrentLinkedQueue<Thread> queue = new ConcurrentLinkedQueue<>();
        final AtomicInteger lockedCounter = new AtomicInteger();

        int threadNum = 5;
        CountDownLatch latch = new CountDownLatch(threadNum);

        for (int i = 0; i < threadNum; i++) {
            Thread t1 = new Thread(() -> {
                queue.add(Thread.currentThread());
                try {
                    redisLock.lock();

                    Thread t = queue.poll();
                    assertThat(t).isEqualTo(Thread.currentThread());

                    Thread.sleep(1000);

                    lockedCounter.incrementAndGet();
                    redisLock.unlock();
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            Thread.sleep(10);
            t1.start();
        }

        latch.await(10, TimeUnit.SECONDS);
        assertThat(lockedCounter.get()).isEqualTo(threadNum);
    }

    private static class UnsafeCounter {
        private int count;

        private void increment() {
            count++;
        }
    }
}
