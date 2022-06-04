package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.InstanceId;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.test.redis.RedisCommandFactoryExtensions;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class RedisLockTest {

    private Config config;
    private Timer timer;

    private RedisLock redisLock;
    private RedisSubscriptionService subscriptionService;


    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisCommandFactoryExtensions.jedisPoolCommandFactory, true)
        );
    }

    @BeforeEach
    void beforeEach() {
        config = Config.DEFAULT_CONFIG;
        timer = new HashedWheelTimer();
    }

    @AfterEach
    void afterEach() {
        subscriptionService.stop();
    }

    private void initLock(String lock, RedisCommandFactory commandFactory, boolean blocking) {
        subscriptionService = new RedisSubscriptionService(config, commandFactory, timer, blocking);
        subscriptionService.start();

        redisLock = new RedisLock(lock, InstanceId.VALUE, timer, config, new LockSubscription(subscriptionService), commandFactory);
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testLock(RedisCommandFactory commandFactory, boolean blocking) throws InterruptedException {
        initLock("ll", commandFactory, blocking);

        redisLock.lock(30, TimeUnit.SECONDS);
    }
}
