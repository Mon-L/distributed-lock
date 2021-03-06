package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import java.util.List;

public class JedisPoolCommandFactory implements RedisCommandFactory {

    private final Pool<Jedis> jedisPool;

    public JedisPoolCommandFactory(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.eval(script, keys, args);
        }
    }

    @Override
    public RedisSubscription getSubscription() {
        Jedis jedis = jedisPool.getResource();
        return new JedisSubscription(jedis);
    }

    @Override
    public void stop() {
        if (!jedisPool.isClosed()) {
            jedisPool.close();
        }
    }
}
