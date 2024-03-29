package cn.zcn.distributed.lock.test.redis;

import cn.zcn.distributed.lock.redis.RedisExecutor;
import cn.zcn.distributed.lock.redis.jedis.JedisPoolExecutor;
import cn.zcn.distributed.lock.redis.lettuce.LettuceExecutor;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.yaml.snakeyaml.Yaml;
import redis.clients.jedis.JedisPool;

import java.io.InputStream;
import java.time.Duration;
import java.util.function.Supplier;

public class RedisIntegrationTestContainer {

    private static RedisIntegrationTestConfig redisIntegrationTestConfig;

    private static final NewableLazy<Timer> timer = NewableLazy.of(() -> {
        Timer timer = new HashedWheelTimer();
        ShutdownQueue.register(timer::stop);
        return timer;
    });

    private static final NewableLazy<RedisExecutor> jedisPoolCommandFactory = NewableLazy.of(new Supplier<RedisExecutor>() {
        @Override
        public RedisExecutor get() {
            JedisPool jedisPool;
            if (redisIntegrationTestConfig.getPassword() == null) {
                jedisPool = new JedisPool(redisIntegrationTestConfig.getHost(), redisIntegrationTestConfig.getPort());
            } else {
                jedisPool = new JedisPool(redisIntegrationTestConfig.getHost(), redisIntegrationTestConfig.getPort(), null, redisIntegrationTestConfig.getPassword());
            }

            RedisExecutor redisExecutor = new JedisPoolExecutor(jedisPool);
            ShutdownQueue.register(redisExecutor::stop);
            return redisExecutor;
        }
    });

    private static final NewableLazy<RedisExecutor> lettuceCommandFactory = NewableLazy.of(new Supplier<RedisExecutor>() {
        @Override
        public RedisExecutor get() {
            RedisURI.Builder builder = RedisURI.Builder
                    .redis(redisIntegrationTestConfig.getHost(), redisIntegrationTestConfig.getPort())
                    .withTimeout(Duration.ofSeconds(10));

            if (redisIntegrationTestConfig.getPassword() != null) {
                builder.withPassword(redisIntegrationTestConfig.getPassword().toCharArray()).build();
            }

            RedisURI redisURI = builder.build();
            RedisClient redisClient = RedisClient.create(redisURI);

            RedisExecutor redisExecutor = new LettuceExecutor(redisClient);
            ShutdownQueue.register(redisExecutor::stop);
            return redisExecutor;
        }
    });

    private static final NewableLazy<RedisSubscriptionService> jedisPoolSubscriptionService = NewableLazy.of(() -> {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(timer.get(), jedisPoolCommandFactory.get());
        subscriptionService.start();
        ShutdownQueue.register(subscriptionService::stop);
        return subscriptionService;
    });

    private static final NewableLazy<RedisSubscriptionService> lettuceSubscriptionService = NewableLazy.of(() -> {
        RedisSubscriptionService subscriptionService = new RedisSubscriptionService(timer.get(), lettuceCommandFactory.get());
        subscriptionService.start();
        ShutdownQueue.register(subscriptionService::stop);
        return subscriptionService;
    });

    static {
        InputStream in = RedisIntegrationTestContainer.class.getClassLoader().getResourceAsStream("test.yaml");
        if (in != null) {
            Yaml yaml = new Yaml();
            try {
                redisIntegrationTestConfig = yaml.loadAs(in, RedisIntegrationTestConfig.class);
            } catch (Throwable t) {
                t.printStackTrace();
                useDefaultConfig();
            }
        } else {
            useDefaultConfig();
        }
    }

    private static void useDefaultConfig() {
        redisIntegrationTestConfig = new RedisIntegrationTestConfig();
        redisIntegrationTestConfig.setHost("127.0.0.1");
        redisIntegrationTestConfig.setPort(6379);
    }

    public static Timer getTimer() {
        return timer.get();
    }

    public static RedisExecutor getJedisPoolCommandFactory() {
        return jedisPoolCommandFactory.get();
    }

    public static RedisExecutor getLettuceCommandFactory() {
        return lettuceCommandFactory.get();
    }

    public static RedisSubscriptionService getJedisSubscriptionService() {
        return jedisPoolSubscriptionService.get();
    }

    public static RedisSubscriptionService getLettuceSubscriptionService() {
        return lettuceSubscriptionService.get();
    }

    private static class NewableLazy<T> {
        private T value;
        private final Supplier<T> supplier;

        private NewableLazy(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        private static <T> NewableLazy<T> of(Supplier<T> supplier) {
            return new NewableLazy<>(supplier);
        }

        private T get() {
            if (value == null) {
                value = supplier.get();
            }
            return value;
        }
    }
}
