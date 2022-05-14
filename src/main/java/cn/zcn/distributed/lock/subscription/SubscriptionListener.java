package cn.zcn.distributed.lock.subscription;

public interface SubscriptionListener {
    void onMessage(String channel, Object message);
}
