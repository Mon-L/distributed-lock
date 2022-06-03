package cn.zcn.distributed.lock.redis.jedis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import redis.clients.jedis.UnifiedJedis;

import java.util.List;

public class UnifiedJedisCommandFactory implements RedisCommandFactory {

    private final UnifiedJedis unifiedJedis;

    public UnifiedJedisCommandFactory(UnifiedJedis unifiedJedis) {
        this.unifiedJedis = unifiedJedis;
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return unifiedJedis.eval(script, keys, args);
    }

    @Override
    public RedisSubscription getSubscription() {
        return new UnifiedJedisSubscription(unifiedJedis);
    }

    @Override
    public void stop() {
        unifiedJedis.close();
    }
}
