package cn.zcn.distributed.lock.redis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;

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
    public void testSubscribe() {
        Jedis jedis = jedisPool.getResource();
        new Thread(new Runnable() {
            private int i = 0;

            @Override
            public void run() {
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        System.out.println("s1," + i++);
                    }
                }, "f");
            }
        }).start();

        new Thread(new Runnable() {
            private int i = 0;

            @Override
            public void run() {
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        System.out.println("s2," + i++);
                    }
                }, "f");
            }
        }).start();

        try {
            Thread.sleep(1000 * 5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {

            while (true) {
                Jedis j = jedisPool.getResource();
                j.publish("f", "fdsaf");
                j.publish("f2", "fdsaf");

                j.close();
                Thread.sleep(1000 * 5);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
