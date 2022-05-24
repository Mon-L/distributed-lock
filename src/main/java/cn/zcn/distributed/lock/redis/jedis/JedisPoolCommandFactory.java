package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.exception.LockException;
import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class JedisPoolCommandFactory implements RedisCommandFactory {

    private final JedisPool jedisPool;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private RedisSubscription subscription;

    public JedisPoolCommandFactory(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.eval(script, keys, args);
        }
    }

    @Override
    public void subscribe(RedisSubscriptionListener listener, byte[]... channel) {
        if (isSubscribed.compareAndSet(false, true)) {
            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();
                JedisSubscription jedisSubscription = new JedisSubscription(listener);
                this.subscription = jedisSubscription;
                jedis.subscribe(jedisSubscription.getJedisPubSub(), channel);
            } finally {
                if (jedis != null) {
                    jedis.close();
                    this.subscription = null;
                }
                isSubscribed.set(false);
            }
        } else {
            throw new LockException("Already subscribe redis channel.");
        }
    }

    @Override
    public RedisSubscription getSubscription() {
        return subscription;
    }
}
