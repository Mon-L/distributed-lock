package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class UnifiedJedisCommandFactoryTest {

    private UnifiedJedis unifiedJedis;
    private RedisCommandFactory commandFactory;

    @BeforeEach
    void beforeEach() {
        unifiedJedis = mock(UnifiedJedis.class);
        commandFactory = spy(new UnifiedJedisCommandFactory(unifiedJedis));
    }

    @Test
    public void testEval() {
        byte[] script = new byte[0];
        List<byte[]> keys = Collections.emptyList();
        List<byte[]> args = Collections.emptyList();

        commandFactory.eval(script, keys, args);

        verify(unifiedJedis, times(1)).eval(script, keys, args);
    }

    @Test
    public void testGetSubscription() {
        RedisSubscription redisSubscription = commandFactory.getSubscription();
        assertThat(redisSubscription).isNotNull();
    }

    @Test
    public void testStop() {
        commandFactory.stop();
        verify(unifiedJedis, times(1)).close();
    }
}
