# distributed-lock
基于 Redis、Zookeeper 的分布式锁实现。支持公平锁、非公平锁

## Quick start

### Jedis

``` java
DistributedLockClient lockClient = DistributedLockClient.withJedis()
  .withPool(() -> new JedisPool("127.0.0.1", 6379, null, "123456"))
  .build();

//获取非公平锁
Lock lock = lockClient.getLock("123");

//获取公平锁
//Lock lock = lockClient.getFairLock("123");

lock.lock(10, TimeUnit.SECONDS);

lock.unlock();

```

### Lettuce

``` java
DistributedLockClient lockClient = DistributedLockClient.withLettuce()
  .withRedisClient(() -> {
      RedisURI redisURI = RedisURI.Builder
              .redis("127.0.0.1", 6379)
              .withPassword("123456".toCharArray())
              .build();

      return RedisClient.create(redisURI);
  })
  .build();

//获取非公平锁
Lock lock = lockClient.getLock("123");

//获取公平锁
//Lock lock = lockClient.getFairLock("123");

lock.lock(10, TimeUnit.SECONDS);

lock.unlock();

```

**该项目仅用于学习交流**
