package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class JedisPoolCommandFactoryTest {

    private JedisPool jedisPool;
    private RedisCommandFactory commandFactory;

    @BeforeEach
    void beforeEach() {
        jedisPool = mock(JedisPool.class);
        commandFactory = spy(new JedisPoolCommandFactory(jedisPool));
    }

    @Test
    public void testEval() {
        Jedis jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);

        byte[] script = new byte[0];
        List<byte[]> keys = Collections.emptyList();
        List<byte[]> args = Collections.emptyList();

        commandFactory.eval(script, keys, args);

        verify(jedisPool, times(1)).getResource();
        verify(jedis, times(1)).eval(script, keys, args);
        verify(jedis, times(1)).close();
    }

    @Test
    public void testGetSubscription() {
        Jedis jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);

        RedisSubscription redisSubscription = commandFactory.getSubscription();

        verify(jedisPool, times(1)).getResource();
        assertThat(redisSubscription).isNotNull();
    }

    @Test
    public void testStop() {
        Jedis jedis = mock(Jedis.class);
        when(jedisPool.getResource()).thenReturn(jedis);

        commandFactory.stop();

        verify(jedisPool, times(1)).close();
    }
}
