package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscription;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.List;

public class LettuceClusterCommandFactory implements RedisCommandFactory {

    private final RedisClusterClient redisClient;
    private final StatefulRedisClusterConnection<byte[], byte[]> conn;
    private final RedisAdvancedClusterCommands<byte[], byte[]> commands;

    public LettuceClusterCommandFactory(RedisClusterClient redisClient) {
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
    public boolean isBlocked() {
        return false;
    }

    @Override
    public void stop() {
        if (conn.isOpen()) {
            conn.close();
        }

        redisClient.shutdown();
    }
}
