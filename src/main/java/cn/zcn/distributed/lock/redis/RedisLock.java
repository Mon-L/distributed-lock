package cn.zcn.distributed.lock.redis;

import cn.zcn.distributed.lock.AbstractLock;
import cn.zcn.distributed.lock.subscription.LockSubscription;
import io.netty.util.Timer;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * 非公平锁的 Redis 数据结构
 * <p>
 * 持有锁的对象(string):
 * <pre>
 * "distributed-lock:{lock}": {
 *     "{client-id}:{thread-id}" : {count}
 * }
 * </pre>
 */
class RedisLock extends AbstractLock {

    private final RedisCommandFactory commandFactory;

    public RedisLock(String lock, String clientId, Timer timer, LockSubscription lockSubscription, RedisCommandFactory commandFactory) {
        super(lock, clientId, timer, lockSubscription);
        this.commandFactory = commandFactory;
    }

    @Override
    public boolean isHeldByCurrentThread() {
        long threadId = Thread.currentThread().getId();
        String script = "return redis.call('hexists', KEYS[1], ARGV[1])";

        Long ret = eval(script,
                Collections.singletonList(lockEntryName.getBytes()),
                getLockHolderEntry(threadId).getBytes()
        );

        return ret != null && ret == 1;
    }

    @Override
    protected Long doLock(long durationMillis, long threadId) {
        String script = "if(redis.call('exists', KEYS[1]) == 0) then " +
                "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                "return nil;" +
                "end;" +
                "if(redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                "return nil;" +
                "end;" +
                "return redis.call('pttl', KEYS[1]);";

        return eval(script,
                Collections.singletonList(lockEntryName.getBytes()),
                String.valueOf(durationMillis).getBytes(),
                getLockHolderEntry(threadId).getBytes()
        );
    }

    @Override
    protected boolean doRenew(long durationMillis, long threadId) {
        String script = "if(redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                "return 1;" +
                "end;" +
                "return 0;";

        Long ret = eval(script,
                Collections.singletonList(lockEntryName.getBytes()),
                String.valueOf(durationMillis).getBytes(),
                getLockHolderEntry(threadId).getBytes()
        );

        return ret != null && ret == 1;
    }

    @Override
    protected boolean doUnLock(long threadId) {
        String script = "if(redis.call('hexists', KEYS[1], ARGV[2]) == 0) then " +
                "return nil;" +
                "end;" +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1);" +
                "if(counter <= 0) then " +
                "redis.call('del', KEYS[1]);" +
                "redis.call('publish', KEYS[1], ARGV[1]);" +
                "return 1;" +
                "else " +
                "return 0;" +
                "end;" +
                "return nil;";

        Long ret = eval(
                script,
                Collections.singletonList(lockEntryName.getBytes()),
                RedisSubscriptionListener.UNLOCK_MESSAGE, getLockHolderEntry(threadId).getBytes()
        );

        return ret != null && ret == 1;
    }

    protected Long eval(String script, List<byte[]> keys, byte[]... args) {
        return (Long) commandFactory.eval(script.getBytes(), keys, Arrays.asList(args));
    }
}
