package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;

public interface LockSubscriptionService {

    CompletableFuture<?> subscribe(String channel, SubscriptionListener listener);

    CompletableFuture<?> unsubscribe(String channel);
}
