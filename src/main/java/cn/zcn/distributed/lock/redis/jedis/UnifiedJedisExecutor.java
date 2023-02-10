package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisExecutor;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscription;
import redis.clients.jedis.UnifiedJedis;

import java.util.List;

public class UnifiedJedisExecutor implements RedisExecutor {

    private final UnifiedJedis unifiedJedis;

    public UnifiedJedisExecutor(UnifiedJedis unifiedJedis) {
        this.unifiedJedis = unifiedJedis;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return unifiedJedis.eval(script, keys, args);
    }

    @Override
    public RedisSubscription createSubscription() {
        return new UnifiedJedisSubscription(unifiedJedis);
    }

    @Override
    public boolean isBlocked() {
        return true;
    }

    @Override
    public void stop() {
        unifiedJedis.close();
    }
}
