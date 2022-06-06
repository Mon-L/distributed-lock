package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.subscription.LockMessageListener;
import cn.zcn.distributed.lock.test.redis.RedisCommandFactoryExtensions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisSubscriptionServiceTest {

    static Stream<Arguments> testParams() {
        return Stream.of(
                Arguments.of(RedisCommandFactoryExtensions.jedisPoolCommandFactory, true),
                Arguments.of(RedisCommandFactoryExtensions.lettuceCommandFactory, false)
        );
    }

    private final String lock = "lock";
    private LockMessageListener listener;

    @BeforeEach
    void beforeEach() {
        listener = (channel, message) -> {
        };
    }

    @ParameterizedTest
    @MethodSource("testParams")
    void testSubscribe(RedisCommandFactory commandFactory, boolean isBlocking) {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(commandFactory, isBlocking);
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
    void testUnsubscribe(RedisCommandFactory commandFactory, boolean isBlocking) {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(commandFactory, isBlocking);
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
    void testStop(RedisCommandFactory commandFactory, boolean isBlocking) throws InterruptedException {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(commandFactory, isBlocking);
        subscriptionService.start();

        subscriptionService.subscribe(lock, listener);
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(1);

        subscriptionService.stop();
        TimeUnit.SECONDS.sleep(1);
        assertThat(subscriptionService.getSubscribedChannels()).isEqualTo(0);
    }
}
