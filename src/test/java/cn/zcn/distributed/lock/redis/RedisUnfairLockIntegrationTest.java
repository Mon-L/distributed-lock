package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.redis.subscription.LockSubscription;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionService;
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

public class RedisUnfairLockIntegrationTest {

    private RedisUnfairLock redisLock;

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisIntegrationTestContainer.getJedisPoolCommandFactory(), RedisIntegrationTestContainer.getJedisSubscriptionService()),
                Arguments.of(RedisIntegrationTestContainer.getLettuceCommandFactory(), RedisIntegrationTestContainer.getLettuceSubscriptionService())
        );
    }

    private void initLock(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) {
        redisLock = new RedisUnfairLock(UUID.randomUUID().toString(), ClientId.create(), RedisIntegrationTestContainer.getTimer(), new LockSubscription(subscriptionService), redisExecutor);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testLock(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(redisExecutor, subscriptionService);

        long startTime = System.currentTimeMillis();
        redisLock.lock(1, TimeUnit.SECONDS);

        assertThat(System.currentTimeMillis() - startTime).isLessThan(150);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUnLock(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(redisExecutor, subscriptionService);

        redisLock.lock(10, TimeUnit.SECONDS);
        assertThat(redisLock.isHeldByCurrentThread()).isTrue();

        redisLock.unlock();
        assertThat(redisLock.isHeldByCurrentThread()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUnLockByAnotherThread(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(redisExecutor, subscriptionService);
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
    void testHeldByCurrentThread(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(redisExecutor, subscriptionService);

        assertThat(redisLock.isHeldByCurrentThread()).isFalse();

        redisLock.lock(3, TimeUnit.SECONDS);
        assertThat(redisLock.isHeldByCurrentThread()).isTrue();

        redisLock.unlock();
        assertThat(redisLock.isHeldByCurrentThread()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testConcurrentLock(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(redisExecutor, subscriptionService);

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
    void testTryLockWait(RedisExecutor redisExecutor, RedisSubscriptionService subscriptionService) throws InterruptedException {
        initLock(redisExecutor, subscriptionService);

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
