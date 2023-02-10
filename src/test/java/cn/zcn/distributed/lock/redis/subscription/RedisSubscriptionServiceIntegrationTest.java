package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.redis.RedisExecutor;
import cn.zcn.distributed.lock.test.redis.RedisIntegrationTestContainer;
import io.netty.util.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisSubscriptionServiceIntegrationTest {

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisIntegrationTestContainer.getJedisPoolCommandFactory()),
                Arguments.of(RedisIntegrationTestContainer.getLettuceCommandFactory())
        );
    }

    private final String lock = "lock";
    private final Timer timer = RedisIntegrationTestContainer.getTimer();
    private LockStatusListener listener;

    @BeforeEach
    void beforeEach() {
        listener = (channel, message) -> {
        };
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testSubscribe(RedisExecutor redisExecutor) {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(timer, redisExecutor);
        subscriptionService.start();

        CompletableFuture<Void> promise = subscriptionService.subscribe(lock, listener);

        promise.join();

        assertThat(promise.isDone()).isTrue();
        assertThat(promise.isCompletedExceptionally()).isFalse();
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(1);
        assertThat(subscriptionService.getSubscribedListeners()).isEqualTo(1);

        subscriptionService.stop();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testUnsubscribe(RedisExecutor redisExecutor) {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(timer, redisExecutor);
        subscriptionService.start();

        CompletableFuture<Void> promise;

        promise = subscriptionService.subscribe(lock, listener);
        promise.join();
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(1);

        promise = subscriptionService.unsubscribe(lock);
        promise.join();
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(0);

        subscriptionService.stop();
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testStop(RedisExecutor redisExecutor) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(timer, redisExecutor);
        subscriptionService.start();

        CompletableFuture<Void> subscribePromise = subscriptionService.subscribe(lock, listener);
        subscribePromise.join();
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(1);

        subscriptionService.stop();
        TimeUnit.SECONDS.sleep(1);
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(0);
    }
}
