package cn.zcn.distributed.lock.subscription;

import java.util.concurrent.CompletableFuture;

/**
 * 锁的订阅服务
 */
public interface LockSubscriptionService {

    /**
     * 启动服务
     */
    void start();

    /**
     * 订阅锁
     *
     * @param channel  锁
     * @param listener 订阅监听
     * @return 订阅结果
     */
    CompletableFuture<Void> subscribe(String channel, LockStatusListener listener);

    /**
     * 取消订阅
     *
     * @param channel 锁
     * @return 取消订阅的结果
     */
    CompletableFuture<Void> unsubscribe(String channel);

    /**
     * 停止服务
     */
    void stop();
}
