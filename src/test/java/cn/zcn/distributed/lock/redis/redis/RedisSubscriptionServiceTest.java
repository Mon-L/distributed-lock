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
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class RedisSubscriptionServiceTest {

    static RedisCommandFactory[] testParams() {
        return new RedisCommandFactory[]{
                RedisCommandFactoryExtensions.lettuceCommandFactory
        };
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
    void testSubscribe(RedisCommandFactory commandFactory) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(config, commandFactory, timer);
        subscriptionService.start();

        CompletableFuture<Void> promise = subscriptionService.subscribe(lock, listener);
        TimeUnit.SECONDS.sleep(1);

        assertTrue(promise.isDone());
        assertFalse(promise.isCompletedExceptionally());
        assertEquals(1, subscriptionService.getSubscribedChannels());
        assertEquals(1, subscriptionService.getSubscribedListeners());

        subscriptionService.stop();
    }


    @ParameterizedTest
    @MethodSource("testParams")
    void testUnsubscribe(RedisCommandFactory commandFactory) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(config, commandFactory, timer);
        subscriptionService.start();

        subscriptionService.subscribe(lock, listener);
        TimeUnit.SECONDS.sleep(1);
        assertEquals(1, subscriptionService.getSubscribedChannels());

        subscriptionService.unsubscribe(lock);
        TimeUnit.SECONDS.sleep(1);
        assertEquals(0, subscriptionService.getSubscribedChannels());
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testStop(RedisCommandFactory commandFactory) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(config, commandFactory, timer);
        subscriptionService.start();

        subscriptionService.subscribe(lock, listener);
        assertEquals(1, subscriptionService.getSubscribedChannels());

        subscriptionService.stop();
        TimeUnit.SECONDS.sleep(1);

        assertEquals(0, subscriptionService.getSubscribedChannels());
    }
}
