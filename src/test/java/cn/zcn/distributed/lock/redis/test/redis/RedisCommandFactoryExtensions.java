package cn.zcn.distributed.lock.redis.test.redis;

import cn.zcn.distributed.lock.redis.RedisCommandFactory;
import cn.zcn.distributed.lock.redis.jedis.JedisPoolCommandFactory;
import cn.zcn.distributed.lock.redis.lettuce.LettuceCommandFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.JedisPool;

import java.io.InputStream;
import java.time.Duration;

public class RedisCommandFactoryExtensions {

    private static RedisTestConfig redisTestConfig;
    public static RedisCommandFactory jedisPoolCommandFactory;
    public static RedisCommandFactory lettuceCommandFactory;

    static {
        InputStream in = RedisCommandFactoryExtensions.class.getClassLoader().getResourceAsStream("test.yaml");
        if (in != null) {
            Yaml yaml = new Yaml();
            try {
                redisTestConfig = yaml.loadAs(in, RedisTestConfig.class);
            } catch (Throwable t) {
                t.printStackTrace();
                useDefaultConfig();
            }
        } else {
            useDefaultConfig();
        }

        jedisPoolCommandFactory = createJedisPoolCommandFactory();
        lettuceCommandFactory = createLettuceCommandFactory();
    }

    private static void useDefaultConfig() {
        redisTestConfig = new RedisTestConfig();
        redisTestConfig.setHost("127.0.0.1");
        redisTestConfig.setPort(6379);
    }

    public static RedisTestConfig getRedisConfig() {
        return redisTestConfig;
    }

    public static RedisCommandFactory createJedisPoolCommandFactory() {
        JedisPool jedisPool;
        if (redisTestConfig.getPassword() == null) {
            jedisPool = new JedisPool(redisTestConfig.getHost(), redisTestConfig.getPort());
        } else {
            jedisPool = new JedisPool(redisTestConfig.getHost(), redisTestConfig.getPort(), null, redisTestConfig.getPassword());
        }

        return new JedisPoolCommandFactory(jedisPool);
    }

    public static RedisCommandFactory createLettuceCommandFactory() {
        RedisURI.Builder builder = RedisURI.Builder
                .redis(redisTestConfig.getHost(), redisTestConfig.getPort())
                .withTimeout(Duration.ofSeconds(10));

        if (redisTestConfig.getPassword() != null) {
            builder.withPassword(redisTestConfig.getPassword().toCharArray()).build();
        }

        RedisURI redisURI = builder.build();
        RedisClient redisClient = RedisClient.create(redisURI);

        return new LettuceCommandFactory(redisClient);
    }
}
