package cn.zcn.distributed.lock.redis.subscription;

public interface LockStatusListener {
    void unlock(String channel, Object message);
}
