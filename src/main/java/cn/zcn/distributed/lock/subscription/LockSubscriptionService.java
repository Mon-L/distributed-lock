package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;

public interface LockSubscriptionService {

    CompletableFuture<Void> subscribe(String channel, SubscriptionListener listener);

    CompletableFuture<Void> unsubscribe(String channel);

    void close();
}
