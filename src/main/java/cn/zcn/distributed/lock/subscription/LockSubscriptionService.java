package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;

public interface LockSubscriptionService {

    void start();

    CompletableFuture<Void> subscribe(String channel, LockMessageListener listener);

    CompletableFuture<Void> unsubscribe(String channel);

    void stop();
}
