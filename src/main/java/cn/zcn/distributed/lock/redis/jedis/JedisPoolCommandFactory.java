package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class JedisPoolCommandFactory implements RedisCommandFactory {

    private final JedisPool jedisPool;
    private RedisSubscription subscription;

    public JedisPoolCommandFactory(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        Jedis jedis = jedisPool.getResource();
        return jedis.eval(script, keys, args);
    }

    @Override
    public void subscribe(RedisSubscriptionListener listener, byte[]... channel) {
        Jedis jedis = jedisPool.getResource();
        JedisSubscription jedisSubscription = new JedisSubscription(listener);
        this.subscription = jedisSubscription;
        jedis.subscribe(jedisSubscription.getJedisPubSub(), channel);
    }

    @Override
    public RedisSubscription getSubscription() {
        return subscription;
    }
}
