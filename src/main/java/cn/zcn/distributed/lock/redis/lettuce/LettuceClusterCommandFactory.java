package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.LockException;
import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LettuceClusterCommandFactory implements RedisCommandFactory {

    private final RedisClusterClient redisClient;
    private final StatefulRedisClusterConnection<byte[], byte[]> conn;
    private final RedisAdvancedClusterCommands<byte[], byte[]> commands;
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);

    private RedisSubscription redisSubscription;

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
    public void subscribe(RedisSubscriptionListener listener, byte[]... channel) {
        if (isSubscribed.compareAndSet(false, true)) {
            try {
                redisSubscription = new LettuceSubscription(redisClient.connectPubSub(ByteArrayCodec.INSTANCE), listener);
            } catch (Exception e) {
                redisSubscription = null;
                isSubscribed.set(false);
                throw new LockException("Failed to Subscribe channel.", e);
            }
        } else {
            throw new LockException("Already subscribed; use the subscription to cancel or add new channels");
        }
    }

    @Override
    public RedisSubscription getSubscription() {
        return redisSubscription;
    }

    @Override
    public void stop() {
        if (redisSubscription != null) {
            redisSubscription.close();
        }

        if (conn.isOpen()) {
            conn.close();
        }
    }
}
