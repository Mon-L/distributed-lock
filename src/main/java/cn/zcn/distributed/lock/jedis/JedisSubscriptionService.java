package cn.zcn.distributed.lock.jedis;

import cn.zcn.distributed.lock.subscription.LockSubscriptionEntry;
import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import cn.zcn.distributed.lock.subscription.SubscriptionListener;

import java.util.concurrent.CompletableFuture;

public class JedisSubscriptionService implements LockSubscriptionService {

    @Override
    public CompletableFuture<Void> subscribe(String channel, SubscriptionListener listener, CompletableFuture<LockSubscriptionEntry> promise) {
        return null;
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String channel) {
        return null;
    }
}
