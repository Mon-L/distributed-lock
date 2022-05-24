package cn.zcn.distributed.lock.subscription;

public interface LockMessageListener {
    void onMessage(String channel, Object message);
}
