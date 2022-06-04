package cn.zcn.distributed.lock.redis;

import java.util.List;

public interface RedisCommandFactory {

    Object eval(byte[] script, List<byte[]> keys, List<byte[]> args);

    RedisSubscription getSubscription();

    void stop();
}
