package cn.zcn.distributed.lock.redis.redis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import cn.zcn.distributed.lock.redis.RedisSubscriptionService;
import cn.zcn.distributed.lock.subscription.LockMessageListener;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class RedisSubscriptionServiceTest {

    private final String lock = "lock";
    private final byte[] lockBytes = lock.getBytes(StandardCharsets.UTF_8);
    private RedisSubscriptionService redisSubscriptionService;
    private MockBlockedRedisCommandFactory redisCommandFactory;
    private MockRedisSubscription redisSubscription;

    @BeforeEach
    public void beforeEach() {
        Config config = new Config();

        redisCommandFactory = spy(new MockBlockedRedisCommandFactory());
        redisSubscription = redisCommandFactory.getMockSubscription();
        redisSubscriptionService = spy(new RedisSubscriptionService(config, redisCommandFactory, new HashedWheelTimer()));
        redisSubscriptionService.start();
    }

    @Test
    public void testSubscribeThenSuccess() {
        CompletableFuture<Void> promise = redisSubscriptionService.subscribe(lock, new NoopMessageListener());

        promise.join();
        assertTrue(promise.isDone());
        assertFalse(promise.isCompletedExceptionally());
        verify(redisCommandFactory, times(1)).subscribe(any(), eq(lockBytes));
        verify(redisSubscription, times(0)).subscribe(lockBytes);
    }

    @Test
    public void testUnsubscribeThenSuccess() {
        testSubscribeThenSuccess();
        redisSubscriptionService.unsubscribe("lock");

        verify(redisSubscription, times(1)).unsubscribe(lockBytes);
    }

    @Test
    public void testClose() {
        redisSubscriptionService.stop();
        verify(redisSubscription, times(1)).close();
    }

    private static class NoopMessageListener implements LockMessageListener {
        @Override
        public void onMessage(String channel, Object message) {
        }
    }

    private static class MockBlockedRedisCommandFactory implements RedisCommandFactory {

        private final MockRedisSubscription redisSubscription = spy(new MockRedisSubscription());

        @Override
        public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
            return null;
        }

        @Override
        public void subscribe(RedisSubscriptionListener listener, byte[]... channels) {
            redisSubscription.subscribe(listener, channels);

            try {
                redisSubscription.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public RedisSubscription getSubscription() {
            return redisSubscription;
        }

        public MockRedisSubscription getMockSubscription() {
            return redisSubscription;
        }
    }

    private static class MockRedisSubscription implements RedisSubscription {

        private final int executeDelayedMillis = 300;
        private final Semaphore semaphore = new Semaphore(0);
        private final List<String> subscribeChannels = new ArrayList<>();
        private RedisSubscriptionListener subscriptionListener;

        public void subscribe(RedisSubscriptionListener listener, byte[]... channels) {
            this.subscriptionListener = listener;
            asyncExecute(() -> {
                for (byte[] channel : channels) {
                    subscribeChannels.add(new String(channel));
                    subscriptionListener.onSubscribe(channel, subscribeChannels.size());
                }
            });
        }

        @Override
        public void subscribe(byte[]... channels) {
            asyncExecute(() -> {
                for (byte[] channel : channels) {
                    subscribeChannels.add(new String(channel));
                    subscriptionListener.onSubscribe(channel, subscribeChannels.size());
                }
            });
        }

        @Override
        public void unsubscribe(byte[]... channels) {
            asyncExecute(() -> {
                for (byte[] channel : channels) {
                    subscribeChannels.remove(new String(channel));
                    subscriptionListener.onUnsubscribe(channel, subscribeChannels.size());
                    releaseIfNeed();
                }
            });
        }

        private void releaseIfNeed() {
            if (subscribeChannels.isEmpty()) {
                semaphore.release();
            }
        }

        public void acquire() throws InterruptedException {
            semaphore.acquire();
        }

        @Override
        public void close() {
            asyncExecute(() -> {
                subscribeChannels.clear();
                releaseIfNeed();
            });
        }

        private void asyncExecute(Runnable runnable) {
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(executeDelayedMillis);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                runnable.run();
            });
        }
    }
}
