package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import redis.clients.jedis.UnifiedJedis;

import java.util.List;

public class UnifiedJedisCommandFactory implements RedisCommandFactory {

    private final UnifiedJedis unifiedJedis;
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
        JedisSubscription jedisSubscription = new JedisSubscription(listener);
        this.subscription = jedisSubscription;
        unifiedJedis.subscribe(jedisSubscription.getJedisPubSub(), channel);
    }

    @Override
    public RedisSubscription getSubscription() {
        return subscription;
    }
}
