package cn.zcn.distributed.lock;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisDistributedLockCreator;
import cn.zcn.distributed.lock.redis.jedis.JedisPoolCommandFactory;
import cn.zcn.distributed.lock.redis.jedis.UnifiedJedisCommandFactory;
import com.sun.tools.javac.util.Assert;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.util.function.Supplier;

class JedisDistributedLockClientBuilder implements DistributedLockClientBuilder {

    private Config config = Config.DEFAULT_CONFIG;
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
    public JedisDistributedLockClientBuilder withConfig(Config config) {
        this.config = config;
        return this;
    }

    @Override
    public DistributedLockClient build() {
        Assert.checkNonNull(redisCommandFactory, "Must config jedis instance.");

        RedisDistributedLockCreator redisDistributedLockCreator = new RedisDistributedLockCreator(config, redisCommandFactory);
        redisDistributedLockCreator.start();

        return new DistributedLockClient(redisDistributedLockCreator);
    }
}
