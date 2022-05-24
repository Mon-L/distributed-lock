package cn.zcn.distributed.lock.redis;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class RedisTest {

    private JedisPool jedisPool;

    @BeforeAll
    public void before() {
        jedisPool = new JedisPool("127.0.0.1", 6379, null, "123456");
    }

    @Test
    public void testLock() {
        Jedis jedis = jedisPool.getResource();

        List<String> keys = new ArrayList<>();
        keys.add("-lock-pis");

        List<String> args = new ArrayList<>();
        args.add("30000");
        args.add("873648-7329uu23-gdsafsa2:1");

        String script = "if (redis.call('exists', KEYS[1]) == 0) then " +
                "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "return nil; " +
                "end; " +
                "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "return nil; " +
                "end; " +
                "return redis.call('pttl', KEYS[1]);";

        Object obj = jedis.eval(script, keys, args);
        System.out.println(obj);
    }

    @Test
    public void testSubscribe() throws Exception {
        Jedis jedis = jedisPool.getResource();
        BinaryJedisPubSub jedisPubSub = new BinaryJedisPubSub() {

            @Override
            public void onMessage(byte[] channel, byte[] message) {
                System.out.println("onMessage : " + new String(message));
            }

            @Override
            public void onSubscribe(byte[] channel, int subscribedChannels) {
                System.out.printf("onSubscribe : %s, count : %d\n", new String(channel), subscribedChannels);
            }
        };

        new Thread(new Runnable() {
            @Override
            public void run() {
                jedis.subscribe(jedisPubSub, null);
            }
        }).start();

        Thread.sleep(100000);
    }

    @Test
    public void testCompleteOrder() throws InterruptedException {
        CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(Thread.currentThread().getName());
                return true;
            }
        });

        future.whenComplete((r, t) -> {
            System.out.println("1");
        });

        future.whenComplete((r, t) -> {
            System.out.println("2");
        });

        Thread.sleep(70000);
    }
}
