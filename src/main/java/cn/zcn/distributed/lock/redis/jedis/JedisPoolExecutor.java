package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisExecutor;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscription;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import java.util.List;

public class JedisPoolExecutor implements RedisExecutor {

    private final Pool<Jedis> jedisPool;

    public JedisPoolExecutor(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.eval(script, keys, args);
        }
    }

    @Override
    public RedisSubscription createSubscription() {
        Jedis jedis = jedisPool.getResource();
        return new JedisSubscription(jedis);
    }

    @Override
    public boolean isBlocked() {
        return true;
    }

    @Override
    public void stop() {
        if (!jedisPool.isClosed()) {
            jedisPool.close();
        }
    }
}
