package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.LockFactory;
import cn.zcn.distributed.lock.ClientId;
import cn.zcn.distributed.lock.Lock;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

public class RedisLockFactory implements LockFactory {

    private volatile boolean running;
    private final Timer timer;
    private final LockSubscription lockSubscription;
    private final RedisCommandFactory redisCommandFactory;
    private final RedisSubscriptionService redisSubscriptionService;

    public RedisLockFactory(RedisCommandFactory redisCommandFactory, boolean isBlocking) {
        this.timer = new HashedWheelTimer();
        this.redisCommandFactory = redisCommandFactory;
        this.redisSubscriptionService = new RedisSubscriptionService(redisCommandFactory, isBlocking);
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
        return new RedisLock(name, ClientId.VALUE, timer, lockSubscription, redisCommandFactory);
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
