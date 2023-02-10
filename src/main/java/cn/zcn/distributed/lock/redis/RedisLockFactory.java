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
    private final RedisExecutor redisExecutor;
    private final RedisSubscriptionService redisSubscriptionService;

    public RedisLockFactory(RedisExecutor redisExecutor) {
        this.clientId = ClientId.create();
        this.redisExecutor = redisExecutor;
        this.timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
        this.redisSubscriptionService = new RedisSubscriptionService(timer, redisExecutor);
        this.lockSubscription = new LockSubscription(redisSubscriptionService);
    }

    public void start() {
        if (!running) {
            running = true;
            redisSubscriptionService.start();
        }
    }

    public RedisLock getUnfairLock(String name) throws IllegalStateException {
        checkState();

        return new RedisUnfairLock(name, clientId, timer, lockSubscription, redisExecutor);
    }

    public RedisLock getFairLock(String name) throws IllegalStateException {
        checkState();

        return new RedisFairLock(name, clientId, timer, lockSubscription, redisExecutor);
    }

    private void checkState() throws IllegalStateException {
        if (!running) {
            throw new IllegalStateException("RedisLockFactory is not running.");
        }
    }

    public void shutdown() {
        if (running) {
            timer.stop();
            redisSubscriptionService.stop();
            redisExecutor.stop();
            running = false;
        }
    }
}
