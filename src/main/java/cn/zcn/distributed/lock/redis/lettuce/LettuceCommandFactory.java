package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscription;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.List;

public class LettuceCommandFactory implements RedisCommandFactory {

    private final RedisClient redisClient;
    private final StatefulRedisConnection<byte[], byte[]> conn;
    private final RedisCommands<byte[], byte[]> commands;

    public LettuceCommandFactory(RedisClient redisClient) {
        this.redisClient = redisClient;
        this.conn = redisClient.connect(ByteArrayCodec.INSTANCE);
        this.commands = this.conn.sync();
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return commands.eval(new String(script), ScriptOutputType.INTEGER, keys.toArray(new byte[0][]), args.toArray(new byte[0][]));
    }

    @Override
    public RedisSubscription getSubscription() {
        return new LettuceSubscription(redisClient.connectPubSub(ByteArrayCodec.INSTANCE));
    }

    @Override
    public void stop() {
        if (conn.isOpen()) {
            conn.close();
        }

        redisClient.shutdown();
    }
}
