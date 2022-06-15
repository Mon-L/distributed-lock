package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.ClientId;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.test.redis.RedisIntegrationTestContainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisLockIntegrationTest {

    private RedisLock redisLock;

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisIntegrationTestContainer.getJedisPoolCommandFactory(), RedisIntegrationTestContainer.getJedisSubscriptionService()),
                Arguments.of(RedisIntegrationTestContainer.getLettuceCommandFactory(), RedisIntegrationTestContainer.getLettuceSubscriptionService())
        );
    }

    private void initLock(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) {
        redisLock = new RedisLock(UUID.randomUUID().toString(), ClientId.VALUE, RedisIntegrationTestContainer.getTimer(), new LockSubscription(subscriptionService), commandFactory);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testLock(RedisCommandFactory commandFactory, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(commandFactory, subscriptionService);

        long startTime = System.currentTimeMillis();
        redisLock.lock(1, TimeUnit.SECONDS);

        assertThat(System.currentTimeMillis() - startTime).isLessThan(150);
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

        int threadNum = 10;
        UnsafeCounter counter = new UnsafeCounter();
        CountDownLatch latch = new CountDownLatch(threadNum);

        for (int i = 0; i < threadNum; i++) {
            new Thread(() -> {
                try {
                    redisLock.lock(3, TimeUnit.SECONDS);
                    counter.increment();
                    redisLock.unlock();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(6, TimeUnit.SECONDS);

        assertThat(counter.count).isEqualTo(threadNum);
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

        semaphore.tryAcquire(2, TimeUnit.SECONDS);

        long startTime = System.currentTimeMillis();
        boolean locked = redisLock.tryLock(3500, TimeUnit.MILLISECONDS, 1, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();

        assertThat(locked).isTrue();
        assertThat(endTime - startTime).isBetween(2900L, 3100L);
    }

    private static class UnsafeCounter {
        private int count;

        private void increment() {
            count++;
        }
    }
}
