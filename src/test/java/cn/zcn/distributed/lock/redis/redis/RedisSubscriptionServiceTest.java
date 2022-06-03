package cn.zcn.distributed.lock.redis.redis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscriptionService;
import cn.zcn.distributed.lock.redis.test.redis.RedisCommandFactoryExtensions;
import cn.zcn.distributed.lock.subscription.LockMessageListener;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class RedisSubscriptionServiceTest {

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisCommandFactoryExtensions.jedisPoolCommandFactory, true),
                Arguments.of(RedisCommandFactoryExtensions.lettuceCommandFactory, false)
        );
    }

    private final String lock = "lock";
    private Config config;
    private Timer timer;
    private LockMessageListener listener;

    @BeforeEach
    void beforeEach() {
        config = Config.DEFAULT_CONFIG;
        timer = new HashedWheelTimer();
        listener = (channel, message) -> {
        };
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testSubscribe(RedisCommandFactory commandFactory, boolean isBlocking) {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(config, commandFactory, timer, isBlocking);
        subscriptionService.start();

        CompletableFuture<Void> promise = subscriptionService.subscribe(lock, listener);

        assertTrue(promise.isDone());
        assertFalse(promise.isCompletedExceptionally());
        assertEquals(1, subscriptionService.getSubscribedChannels());
        assertEquals(1, subscriptionService.getSubscribedListeners());

        subscriptionService.stop();
    }


    @ParameterizedTest
    @MethodSource("testParams")
    void testUnsubscribe(RedisCommandFactory commandFactory, boolean isBlocking) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(config, commandFactory, timer, isBlocking);
        subscriptionService.start();

        subscriptionService.subscribe(lock, listener);
        assertEquals(1, subscriptionService.getSubscribedChannels());

        subscriptionService.unsubscribe(lock);
        TimeUnit.SECONDS.sleep(1);
        assertEquals(0, subscriptionService.getSubscribedChannels());

        subscriptionService.stop();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testStop(RedisCommandFactory commandFactory, boolean isBlocking) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(config, commandFactory, timer, isBlocking);
        subscriptionService.start();

        subscriptionService.subscribe(lock, listener);
        assertEquals(1, subscriptionService.getSubscribedChannels());

        subscriptionService.stop();
        TimeUnit.SECONDS.sleep(1);
        assertEquals(0, subscriptionService.getSubscribedChannels());
    }
}
