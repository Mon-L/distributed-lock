package cn.zcn.distributed.lock;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisLockFactory;
import cn.zcn.distributed.lock.redis.lettuce.LettuceClusterCommandFactory;
import cn.zcn.distributed.lock.redis.lettuce.LettuceCommandFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.cluster.RedisClusterClient;

import java.util.function.Supplier;

class LettuceDistributedLockClientBuilder implements DistributedLockClientBuilder {

    private RedisCommandFactory redisCommandFactory;

    public LettuceDistributedLockClientBuilder withRedisClient(Supplier<RedisClient> supplier) {
        redisCommandFactory = new LettuceCommandFactory(supplier.get());
        return this;
    }

    public LettuceDistributedLockClientBuilder withRedisClusterClient(Supplier<RedisClusterClient> supplier) {
        redisCommandFactory = new LettuceClusterCommandFactory(supplier.get());
        return this;
    }

    @Override
    public DistributedLockClient build() {
        if (redisCommandFactory == null) {
            throw new IllegalStateException("Must config lettuce instance.");
        }

        RedisLockFactory lockFactory = new RedisLockFactory(redisCommandFactory, false);
        lockFactory.start();
        lockFactory.registerShutdownHook();

        return new DistributedLockClient(lockFactory);
    }
}
