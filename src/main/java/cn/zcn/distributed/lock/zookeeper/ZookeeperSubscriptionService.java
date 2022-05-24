package cn.zcn.distributed.lock.zookeeper;

import cn.zcn.distributed.lock.subscription.LockSubscriptionService;
import cn.zcn.distributed.lock.subscription.LockMessageListener;

import java.util.concurrent.CompletableFuture;

public class ZookeeperSubscriptionService implements LockSubscriptionService {

    @Override
    public void start() {
    }

    @Override
    public CompletableFuture<Void> subscribe(String channel, LockMessageListener listener) {
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
