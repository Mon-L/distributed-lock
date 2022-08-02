package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.test.redis.NoopRedisSubscriptionListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatException;
import static org.mockito.Mockito.*;

public class AbstractRedisSubscriptionTest {

    private AbstractRedisSubscription redisSubscription;

    @BeforeEach
    void beforeEach() {
        redisSubscription = spy(new AbstractRedisSubscription() {
            @Override
            protected void doSubscribe(RedisSubscriptionListener listener, byte[]... channels) {
            }

            @Override
            protected void doSubscribe(byte[]... channels) {
            }

            @Override
            protected void doUnsubscribe(byte[]... channels) {
            }

            @Override
            public long getSubscribedChannels() {
                return 0;
            }

            @Override
            public boolean isAlive() {
                return false;
            }

            @Override
            public void close() {

            }
        });
    }

    @Test
    void testSubscribe() {
        RedisSubscriptionListener listener = new NoopRedisSubscriptionListener();
        byte[] channels = "l".getBytes();

        redisSubscription.subscribe(listener, channels);
        verify(redisSubscription, times(1)).doSubscribe(listener, channels);

        redisSubscription.subscribe(channels);
        verify(redisSubscription, times(1)).doSubscribe(channels);
    }

    @Test
    void testUnsubscribe() {
        byte[] channels = "l".getBytes();

        redisSubscription.unsubscribe(channels);
        verify(redisSubscription, times(1)).doUnsubscribe(channels);
    }

    @Test
    void testSubscribeWithNullListener() {
        assertThatException()
                .isThrownBy(() -> redisSubscription.subscribe(null, new byte[][]{"l".getBytes()}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSubscribeWithEmptyChannel() {
        assertThatException()
                .isThrownBy(() -> redisSubscription.subscribe(new NoopRedisSubscriptionListener(), null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatException()
                .isThrownBy(() -> redisSubscription.subscribe(new NoopRedisSubscriptionListener(), new byte[0][0]))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatException()
                .isThrownBy(() -> redisSubscription.subscribe())
                .isInstanceOf(IllegalArgumentException.class);

        assertThatException()
                .isThrownBy(() -> redisSubscription.subscribe(new byte[0][0]))
                .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    void testUnsubscribeWithEmptyChannel() {
        assertThatException()
                .isThrownBy(() -> redisSubscription.unsubscribe())
                .isInstanceOf(IllegalArgumentException.class);

        assertThatException()
                .isThrownBy(() -> redisSubscription.unsubscribe(new byte[0][0]))
                .isInstanceOf(IllegalArgumentException.class);

    }

    @Test
    void testSubscribeRepeated() {
        redisSubscription.subscribe(new NoopRedisSubscriptionListener(), "l".getBytes());

        assertThatException()
                .isThrownBy(() -> redisSubscription.subscribe(new NoopRedisSubscriptionListener(), "l".getBytes()))
                .isInstanceOf(IllegalStateException.class);
    }
}
