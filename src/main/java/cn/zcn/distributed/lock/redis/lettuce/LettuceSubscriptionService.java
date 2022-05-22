package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import cn.zcn.distributed.lock.subscription.LockSubscriptionListener;

import java.util.concurrent.CompletableFuture;

public class LettuceSubscriptionService implements LockSubscriptionService {

    @Override
    public void start() {

    }

    @Override
    public CompletableFuture<Void> subscribe(String channel, LockSubscriptionListener listener) {
        return null;
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String channel) {
        return null;
    }

    @Override
    public void stop() {

    }
}
