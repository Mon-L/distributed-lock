package cn.zcn.distributed.lock.subscription;

public interface LockStatusListener {
    void unlock(String channel, Object message);
}
