package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.test.redis.NoopRedisSubscriptionListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
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
        assertThrows(IllegalArgumentException.class, () -> redisSubscription.subscribe(null, new byte[][]{"l".getBytes()}));
    }

    @Test
    void testSubscribeWithEmptyChannel() {
        assertThrows(IllegalArgumentException.class, () -> redisSubscription.subscribe(new NoopRedisSubscriptionListener(), null));
        assertThrows(IllegalArgumentException.class, () -> redisSubscription.subscribe(new NoopRedisSubscriptionListener(), new byte[0][0]));

        assertThrows(IllegalArgumentException.class, () -> redisSubscription.subscribe());
        assertThrows(IllegalArgumentException.class, () -> redisSubscription.subscribe(new byte[0][0]));
    }

    @Test
    void testUnsubscribeWithEmptyChannel() {
        assertThrows(IllegalArgumentException.class, () -> redisSubscription.unsubscribe());
        assertThrows(IllegalArgumentException.class, () -> redisSubscription.unsubscribe(new byte[0][0]));
    }

    @Test
    void testSubscribeRepeated() {
        redisSubscription.subscribe(new NoopRedisSubscriptionListener(), "l".getBytes());
        assertThrows(IllegalStateException.class, () -> redisSubscription.subscribe(new NoopRedisSubscriptionListener(), "l".getBytes()));
    }

}
