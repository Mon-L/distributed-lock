package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;

import java.util.List;

public class LettuceCommandFactory implements RedisCommandFactory {
    
    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return null;
    }

    @Override
    public void subscribe(RedisSubscriptionListener listener, byte[]... channel) {

    }

    @Override
    public RedisSubscription getSubscription() {
        return null;
    }
}
