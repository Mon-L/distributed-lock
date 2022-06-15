package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.subscription.LockSubscription;
import cn.zcn.distributed.lock.subscription.LockSubscriptionHolder;
import io.netty.util.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 公平锁的 Redis 数据结构
 * <p>
 * 持有锁的对象(string):
 * <pre>
 * "distributed-lock:{lock}": {
 *     "{client-id}:{thread-id}" : {count}
 * }
 * </pre>
 * <p>
 * 竞争锁对象的队列(list):
 * <pre>
 * "distributed-lock:{lock}:queen": [
 *     "{client-id}:{thread-id}"
 * ]
 * </pre>
 * <p>
 * 竞争锁的超时时间集合(zset):
 * <pre>
 * "distributed-lock:{lock}:timeout": [
 *      {
 *          value: {client-id}:{thread-id},
 *          scope: timeout(millis)
 *      }
 * ]
 * </pre>
 */
public class RedisFairLock extends RedisLock {

    private final String queenName;
    private final String timeoutSetName;

    public RedisFairLock(String lock, String clientId, Timer timer, LockSubscription lockSubscription, RedisCommandFactory commandFactory) {
        super(lock, clientId, timer, lockSubscription, commandFactory);
        queenName = withLockPrefix(lock + ":queen");
        timeoutSetName = withLockPrefix(lock + ":timeout");
    }

    @Override
    protected Long doLock(long durationMillis, long threadId) {
        String script = "while(true) do" +
                "    local firstThreadId = redis.call('lindex', KEYS[2], 0);" +
                "    if firstThreadId == false then " +
                "        break;" +
                "    end;" +
                "    local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId));" +
                "    if timeout <= tonumber(ARGV[4]) then " +
                "        redis.call('lpop', KEYS[2]);" +
                "        redis.call('zrem', KEYS[3], firstThreadId);" +
                "    else" +
                "        break;" +
                "    end;" +
                "end;" +
                "if( redis.call('exists', KEYS[1]) == 0 and " +
                "    (redis.call('exists', KEYS[2]) == 0 or redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                "" +
                "    redis.call('lpop', KEYS[2]);" +
                "    redis.call('zrem', KEYS[3], ARGV[2]);" +
                "" +
                "    local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                "    for i = 1, #keys, 1 do" +
                "        redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                "    end;" +
                "    redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                "    redis.call('pexpire', KEYS[1], ARGV[1]);" +
                "    return nil;" +
                "end;" +
                "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
                "    redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                "    redis.call('pexpire', KEYS[1], ARGV[1]);" +
                "    return nil;" +
                "end;" +
                "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                "if timeout ~= false then " +
                "    return timeout - tonumber(ARGV[4]) - tonumber(ARGV[3]);" +
                "end;" +
                "local ttl;" +
                "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                "    ttl = redis.call('zscore', KEYS[3], lastThreadId) - tonumber(ARGV[4]);" +
                "else" +
                "    ttl = redis.call('pttl', KEYS[1]);" +
                "end;" +
                "" +
                "local timeout = tonumber(ARGV[4]) + ttl + tonumber(ARGV[3]);" +
                "redis.call('rpush', KEYS[2], ARGV[2]);" +
                "redis.call('zadd', KEYS[3], timeout, ARGV[2]);" +
                "" +
                "return ttl;";

        List<byte[]> keys = new ArrayList<>();
        keys.add(lockEntryName.getBytes());
        keys.add(queenName.getBytes());
        keys.add(timeoutSetName.getBytes());

        return eval(script,
                keys,
                String.valueOf(durationMillis).getBytes(),
                getLockHolderEntry(threadId).getBytes(),
                String.valueOf(DEFAULT_LOCK_DURATION).getBytes(),
                String.valueOf(System.currentTimeMillis()).getBytes()
        );
    }

    @Override
    protected boolean doUnLock(long threadId) {
        String script = "while(true) do" +
                "    local firstThreadId = redis.call('lindex', KEYS[2], 0);" +
                "    if firstThreadId == false then " +
                "        break;" +
                "    end;" +
                "    local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId));" +
                "    if timeout <= tonumber(ARGV[4]) then " +
                "        redis.call('lpop', KEYS[2]);" +
                "        redis.call('zrem', KEYS[3], firstThreadId);" +
                "    else" +
                "        break;" +
                "    end;" +
                "end;" +
                "if redis.call('exists', KEYS[1]) == 0 then " +
                "    local firstThreadId = redis.call('lindex', KEYS[2], 0);" +
                "    if firstThreadId ~= false then " +
                "        redis.call('publish', KEYS[4] .. ':' .. firstThreadId, ARGV[1]);" +
                "    end;" +
                "    return 1;" +
                "end;" +
                "if redis.call('hexists', KEYS[1], ARGV[3]) == 0 then " +
                "    return nil;" +
                "end;" +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1);" +
                "if counter > 0 then " +
                "    redis.call('pexpire', KEYS[1], ARGV[2]);" +
                "    return 0;" +
                "end;" +
                "redis.call('del', KEYS[1]);" +
                "local firstThreadId = redis.call('lindex', KEYS[2], 0);" +
                "if firstThreadId ~= false then " +
                "    redis.call('publish', KEYS[4] .. ':' .. firstThreadId, ARGV[1]);" +
                "end;" +
                "return 1;";

        List<byte[]> keys = new ArrayList<>();
        keys.add(lockEntryName.getBytes());
        keys.add(queenName.getBytes());
        keys.add(timeoutSetName.getBytes());
        keys.add(lockRawName.getBytes());

        Long ret = eval(
                script,
                keys,
                RedisSubscriptionListener.UNLOCK_MESSAGE,
                String.valueOf(DEFAULT_LOCK_DURATION).getBytes(),
                getLockHolderEntry(threadId).getBytes(),
                String.valueOf(System.currentTimeMillis()).getBytes()
        );

        return ret != null && ret == 1;
    }

    @Override
    protected CompletableFuture<LockSubscriptionHolder> subscribe(long threadId) {
        return lockSubscription.subscribe(lockRawName + ":" + clientId + ":" + threadId);
    }

    @Override
    protected void unsubscribe(LockSubscriptionHolder lockSubscriptionHolder, long threadId) {
        lockSubscription.unsubscribe(lockSubscriptionHolder, lockRawName + ":" + clientId + ":" + threadId);
    }
}
