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

    public RedisLockFactory(RedisCommandFactory redisCommandFactory) {
        this.clientId = ClientId.create();
        this.redisCommandFactory = redisCommandFactory;
        this.timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
        this.redisSubscriptionService = new RedisSubscriptionService(timer, redisCommandFactory);
        this.lockSubscription = new LockSubscription(redisSubscriptionService);
    }

    public void start() {
        if (!running) {
            running = true;
            redisSubscriptionService.start();
        }
    }

    public RedisLock getLock(String name) throws IllegalStateException {
        checkRunning();

        return new RedisLockImpl(name, clientId, timer, lockSubscription, redisCommandFactory);
    }

    public RedisLock getFairLock(String name) throws IllegalStateException {
        checkRunning();

        return new RedisFairLockImpl(name, clientId, timer, lockSubscription, redisCommandFactory);
    }

    private void checkRunning() throws IllegalStateException {
        if (!running) {
            throw new IllegalStateException("RedisLockFactory is not running");
        }
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
