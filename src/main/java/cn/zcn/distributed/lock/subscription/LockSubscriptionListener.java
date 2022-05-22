package cn.zcn.distributed.lock.subscription;

public interface LockSubscriptionListener {
    void onMessage(String channel, Object message);
}
