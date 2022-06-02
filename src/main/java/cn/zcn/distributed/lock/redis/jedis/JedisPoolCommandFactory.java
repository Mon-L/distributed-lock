package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.LockException;
import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.Pool;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class JedisPoolCommandFactory implements RedisCommandFactory {

    private final Pool<Jedis> jedisPool;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);

    private RedisSubscription subscription;

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
    public void subscribe(RedisSubscriptionListener listener, byte[]... channel) {
        if (isSubscribed.compareAndSet(false, true)) {
            try (Jedis jedis = jedisPool.getResource()) {
                JedisSubscription jedisSubscription = new JedisSubscription(listener);
                subscription = jedisSubscription;
                jedis.subscribe(jedisSubscription.getJedisPubSub(), channel);
            } catch (Exception e) {
                throw new LockException("Failed to subscribe channel.", e);
            } finally {
                subscription = null;
                isSubscribed.set(false);
            }
        } else {
            throw new LockException("Already subscribed; use the subscription to cancel or add new channels");
        }
    }

    @Override
    public RedisSubscription getSubscription() {
        return subscription;
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.close();
        }

        jedisPool.close();
    }
}
