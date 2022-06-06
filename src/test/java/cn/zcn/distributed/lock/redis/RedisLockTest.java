package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.ClientId;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.test.redis.RedisCommandFactoryExtensions;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisLockTest {

    private Timer timer;
    private RedisLock redisLock;
    private RedisSubscriptionService subscriptionService;

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisCommandFactoryExtensions.jedisPoolCommandFactory, true),
                Arguments.of(RedisCommandFactoryExtensions.lettuceCommandFactory, true)
        );
    }

    @BeforeEach
    void beforeEach() {
        timer = new HashedWheelTimer();
    }

    @AfterEach
    void afterEach() {
        subscriptionService.stop();
        timer.stop();
    }

    private void initLock(String lock, RedisCommandFactory commandFactory, boolean blocking) {
        subscriptionService = new RedisSubscriptionService(commandFactory, blocking);
        subscriptionService.start();

        redisLock = new RedisLock(lock, ClientId.VALUE, timer, new LockSubscription(subscriptionService), commandFactory);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testLock(RedisCommandFactory commandFactory, boolean blocking) throws InterruptedException {
        initLock("ll", commandFactory, blocking);

        long startTime = System.currentTimeMillis();
        redisLock.lock(3, TimeUnit.SECONDS);

        assertThat(System.currentTimeMillis() - startTime).isLessThan(400);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUnLock(RedisCommandFactory commandFactory, boolean blocking) throws InterruptedException {
        initLock("ll", commandFactory, blocking);
        CountDownLatch latch = new CountDownLatch(1);

        redisLock.lock(10, TimeUnit.SECONDS);

        new Thread(() -> {
            redisLock.unlock();
            latch.countDown();
        }).start();

        latch.await();

        assertThat(redisLock.isHeldByCurrentThread()).isTrue();

        redisLock.unlock();

        assertThat(redisLock.isHeldByCurrentThread()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testHeldByCurrentThread(RedisCommandFactory commandFactory, boolean blocking) throws InterruptedException {
        initLock("ll", commandFactory, blocking);

        assertThat(redisLock.isHeldByCurrentThread()).isFalse();

        redisLock.lock(3, TimeUnit.SECONDS);
        assertThat(redisLock.isHeldByCurrentThread()).isTrue();

        redisLock.unlock();
        assertThat(redisLock.isHeldByCurrentThread()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testConcurrentLock(RedisCommandFactory commandFactory, boolean blocking) throws InterruptedException {
        initLock("ll", commandFactory, blocking);

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

        latch.await(10, TimeUnit.SECONDS);

        assertThat(counter.count).isEqualTo(20);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testTryLockWait(RedisCommandFactory commandFactory, boolean blocking) throws InterruptedException {
        initLock("ll", commandFactory, blocking);

        CountDownLatch latch = new CountDownLatch(1);

        new Thread(() -> {
            try {
                redisLock.lock(3, TimeUnit.SECONDS);

                latch.countDown();
                TimeUnit.MILLISECONDS.sleep(2900);

                redisLock.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        latch.await();

        long startTime = System.currentTimeMillis();
        boolean locked = redisLock.tryLock(3, TimeUnit.SECONDS, 3, TimeUnit.SECONDS);
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
