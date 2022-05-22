package cn.zcn.distributed.lock.redis;

public interface RedisSubscription {

    void subscribe(byte[]... channel);

    void unsubscribe(byte[]... channel);

    void close();
}
