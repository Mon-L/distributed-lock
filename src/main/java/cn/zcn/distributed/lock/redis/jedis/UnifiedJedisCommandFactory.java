package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.exception.LockException;
import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.UnifiedJedis;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class UnifiedJedisCommandFactory implements RedisCommandFactory {

    private final UnifiedJedis unifiedJedis;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private RedisSubscription subscription;

    public UnifiedJedisCommandFactory(UnifiedJedis unifiedJedis) {
        this.unifiedJedis = unifiedJedis;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return unifiedJedis.eval(script, keys, args);
    }

    @Override
    public void subscribe(RedisSubscriptionListener listener, byte[]... channel) {
        if (isSubscribed.compareAndSet(false, true)) {
            try {
                JedisSubscription jedisSubscription = new JedisSubscription(listener);
                this.subscription = jedisSubscription;
                unifiedJedis.subscribe(jedisSubscription.getJedisPubSub(), channel);
            } finally {
                this.subscription = null;
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
