# distributed-lock
基于 Redis、Zookeeper 的分布式锁实现。支持公平锁、非公平锁

## Quick start

### Jedis

``` java
RedisLockFactory redisLockFactory = new RedisLockFactory(new JedisPoolCommandFactory(new JedisPool("127.0.0.1", 6379)));

//获取非公平锁
Lock lock = redisLockFactory.getLock("123");

//获取公平锁
//Lock lock = redisLockFactory.getFairLock("123");

lock.lock(10, TimeUnit.SECONDS);

lock.unlock();

```

### Lettuce

``` java
RedisLockFactory redisLockFactory =  new RedisLockFactory(new LettuceCommandFactory(
    RedisClient.create(RedisURI.Builder.redis("127.0.0.1", 6379).build())));

//获取非公平锁
Lock lock = redisLockFactory.getLock("123");
lock.lock(10, TimeUnit.SECONDS);
lock.unlock();

//获取公平锁
Lock fairLock = redisLockFactory.getFairLock("123");
fairLock.lock(10, TimeUnit.SECONDS);
fairLock.unlock();

```

### Zookeeper

#### 可重入锁
``` java
CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(500, 3));
ZookeeperLock lock = new ZookeeperLock("foo", client);

lock.lock();
lock.unlock();
```

#### 读写锁
``` java
CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(500, 3));
ZookeeperReadWriteLock lock = new ZookeeperReadWriteLock("foo", client);

ZookeeperLock readLock = lock.readLock();
ZookeeperLock writeLock = lock.writeLock();
```

**该项目仅用于学习交流**
