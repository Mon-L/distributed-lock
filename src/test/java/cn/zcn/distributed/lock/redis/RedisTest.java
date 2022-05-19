package cn.zcn.distributed.lock.redis;

import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class RedisTest {

    private JedisPool jedisPool;

    @Before
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
        JedisPubSub jedisPubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                System.out.println("onMessage : " + message);
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                System.out.printf("onSubscribe : %s, count : %d\n", channel, subscribedChannels);
            }
        };

        new Thread(new Runnable() {

            @Override
            public void run() {
                jedis.subscribe(jedisPubSub, "f", "f2", "f3");
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                jedis.subscribe(jedisPubSub, "f", "f2", "f3", "f4");
            }
        }).start();

        Thread.sleep(100000);
    }

    @Test
    public void redisson() throws Exception {
        Config config = new Config();
        config.useSingleServer()
                .setPassword("123456")
                .setAddress("redis://127.0.0.1:6379");
        RedissonClient redisson = Redisson.create(config);
        RLock lock = redisson.getLock("fff");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10000);

                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20000);
                    lock.lockInterruptibly();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
