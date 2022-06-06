package cn.zcn.distributed.lock;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisLockFactory;
import cn.zcn.distributed.lock.redis.jedis.JedisPoolCommandFactory;
import cn.zcn.distributed.lock.redis.jedis.UnifiedJedisCommandFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.function.Supplier;

class JedisDistributedLockClientBuilder implements DistributedLockClientBuilder {

    private RedisCommandFactory redisCommandFactory;

    public JedisDistributedLockClientBuilder withPool(Supplier<JedisPool> supplier) {
        this.redisCommandFactory = new JedisPoolCommandFactory(supplier.get());
        return this;
    }

    public JedisDistributedLockClientBuilder withSentinel(Supplier<JedisSentinelPool> supplier) {
        this.redisCommandFactory = new JedisPoolCommandFactory(supplier.get());
        return this;
    }

    public JedisDistributedLockClientBuilder withCluster(Supplier<JedisCluster> supplier) {
        this.redisCommandFactory = new UnifiedJedisCommandFactory(supplier.get());
        return this;
    }

    @Override
    public DistributedLockClient build() {
        if (redisCommandFactory == null) {
            throw new IllegalStateException("Must config jedis instance.");
        }

        RedisLockFactory lockFactory = new RedisLockFactory(redisCommandFactory, true);
        lockFactory.start();
        lockFactory.registerShutdownHook();

        return new DistributedLockClient(lockFactory);
    }
}
