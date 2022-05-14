package cn.zcn.distributed.lock.lettuce;

import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import cn.zcn.distributed.lock.subscription.SubscriptionListener;

import java.util.concurrent.CompletableFuture;

public class LettuceSubscriptionService implements LockSubscriptionService {

    @Override
    public CompletableFuture<?> subscribe(String channel, SubscriptionListener listener) {
        return null;
    }

    @Override
    public CompletableFuture<?> unsubscribe(String channel) {
        return null;
    }
}
