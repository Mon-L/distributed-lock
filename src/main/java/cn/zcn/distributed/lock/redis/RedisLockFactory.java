package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.redis.subscription.LockSubscription;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionService;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.util.concurrent.TimeUnit;

public class RedisLockFactory {

    private volatile boolean running;
    private final Timer timer;
    private final ClientId clientId;
    private final LockSubscription lockSubscription;
    private final RedisCommandFactory redisCommandFactory;
    private final RedisSubscriptionService redisSubscriptionService;

    public RedisLockFactory(RedisCommandFactory redisCommandFactory, boolean isBlocking) {
        this.clientId = ClientId.create();
        this.timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
        this.redisCommandFactory = redisCommandFactory;
        this.redisSubscriptionService = new RedisSubscriptionService(timer, redisCommandFactory, isBlocking);
        this.lockSubscription = new LockSubscription(redisSubscriptionService);
    }

    public void start() {
        if (!running) {
            running = true;
            redisSubscriptionService.start();
        }
    }

    public RedisLock getLock(String name) {
        return new RedisLockImpl(name, clientId, timer, lockSubscription, redisCommandFactory);
    }

    public RedisLock getFairLock(String name) {
        return new RedisFairLockImpl(name, clientId, timer, lockSubscription, redisCommandFactory);
    }

    public void shutdown() {
        if (running) {
            running = false;
            timer.stop();
            redisSubscriptionService.stop();
            redisCommandFactory.stop();
        }
    }
}
