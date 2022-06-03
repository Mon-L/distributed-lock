package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.Config;
import cn.zcn.distributed.lock.DistributedLockCreator;
import cn.zcn.distributed.lock.InstanceId;
import cn.zcn.distributed.lock.Lock;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

public class RedisDistributedLockCreator implements DistributedLockCreator {

    private volatile boolean running;
    private final Timer timer;
    private final Config config;
    private final LockSubscription lockSubscription;
    private final RedisCommandFactory redisCommandFactory;
    private final RedisSubscriptionService redisSubscriptionService;

    public RedisDistributedLockCreator(Config config, RedisCommandFactory redisCommandFactory, boolean isBlocking) {
        this.config = config;
        this.timer = new HashedWheelTimer();
        this.redisCommandFactory = redisCommandFactory;
        this.redisSubscriptionService = new RedisSubscriptionService(config, redisCommandFactory, timer, isBlocking);
        this.lockSubscription = new LockSubscription(redisSubscriptionService);
    }

    @Override
    public void start() {
        if (!running) {
            running = true;
            redisSubscriptionService.start();
        }
    }

    @Override
    public Lock getLock(String name) {
        return new RedisLock(name, InstanceId.VALUE, timer, config, lockSubscription);
    }

    @Override
    public void shutdown() {
        if (running) {
            running = false;
            timer.stop();
            redisSubscriptionService.stop();
            redisCommandFactory.stop();
        }
    }
}
