package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

public class ZookeeperLockImplTest {

    private TestingServer server;
    private ZookeeperLock lock;
    private CuratorFramework client;

    @BeforeEach
    void before() throws Exception {
        server = new TestingServer();
        server.start();

        final Timing timing = new Timing();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(200, 3);
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), retryPolicy);
        client.start();

        lock = new ZookeeperLockImpl("/test-lock", client);
    }

    @Test
    void testLock() throws Exception {
        assertThat(lock.heldByCurrentThread()).isFalse();

        long startTime = System.currentTimeMillis();
        lock.lock();
        assertThat(System.currentTimeMillis() - startTime).isLessThan(150);
    }

    @Test
    void testHeldByCurrentThread() throws Exception {
        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        lock.unlock();
        assertThat(lock.heldByCurrentThread()).isFalse();

        new Thread(() -> assertThat(lock.heldByCurrentThread()).isFalse()).start();
    }

    @Test
    void testUnlock() throws Exception {
        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        CountDownLatch latch = new CountDownLatch(1);

        //unlock by other thread
        Executors.newSingleThreadExecutor().submit(() -> {
            assertThatException().isThrownBy(() -> lock.unlock()).isInstanceOf(IllegalMonitorStateException.class);
            latch.countDown();
        });

        latch.await(3, TimeUnit.SECONDS);

        assertThat(lock.heldByCurrentThread()).isTrue();

        //unlock
        lock.unlock();
        assertThat(lock.heldByCurrentThread()).isFalse();
    }

    @Test
    void testConcurrentLock() throws Exception {
        int threadNum = 10;
        AtomicBoolean isFirst = new AtomicBoolean(false);
        Semaphore semaphore = new Semaphore(1);

        List<Future<Boolean>> futures = new ArrayList<>();
        ExecutorService service = Executors.newCachedThreadPool();

        for (int i = 0; i < threadNum; i++) {
            Future<Boolean> future = service.submit(() -> {
                if (semaphore.tryAcquire(300, TimeUnit.MILLISECONDS)) {
                    try {
                        lock.lock();

                        if (isFirst.compareAndSet(false, true)) {
                            semaphore.release(threadNum - 1);

                            if (semaphore.availablePermits() > 0) {
                                TimeUnit.MILLISECONDS.sleep(200);
                            }
                        } else {
                            TimeUnit.MILLISECONDS.sleep(200);
                        }
                    } catch (Exception e) {
                        return false;
                    } finally {
                        lock.unlock();
                    }

                    return true;
                } else {
                    return false;
                }
            });
            futures.add(future);
        }

        for (Future<Boolean> f : futures) {
            assertThat(f.get()).isTrue();
        }
    }

    @Test
    void testTryLock() throws Exception {
        assertThat(lock.heldByCurrentThread()).isFalse();

        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        CountDownLatch latch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().submit(() -> {
            boolean locked = false;
            try {
                long startTime = System.currentTimeMillis();
                locked = lock.tryLock(3, TimeUnit.SECONDS);

                assertThat(locked).isTrue();
                assertThat(System.currentTimeMillis() - startTime).isBetween(2000L, 2200L);
            } finally {
                if (locked) {
                    lock.unlock();
                }
                latch.countDown();
            }

            return null;
        });

        TimeUnit.SECONDS.sleep(2);
        lock.unlock();

        latch.await(5, TimeUnit.SECONDS);
    }

    @Test
    void testTryLockThenWaitTimeout() throws Exception {
        assertThat(lock.heldByCurrentThread()).isFalse();

        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        AtomicBoolean done = new AtomicBoolean(false);
        Future<Boolean> future = Executors.newSingleThreadExecutor().submit(() -> {
            boolean locked = false;
            try {
                locked = lock.tryLock(2, TimeUnit.SECONDS);
                return locked;
            } finally {
                done.compareAndSet(false, true);

                if (locked) {
                    lock.unlock();
                }
            }
        });

        long startTime = System.currentTimeMillis();
        future.get(3, TimeUnit.SECONDS);

        assertThat(System.currentTimeMillis() - startTime).isBetween(2000L, 2100L);
        assertThat(done.get()).isTrue();
        assertThat(future.get()).isFalse();
        lock.unlock();
    }

    @Test
    void testReenterLock() throws Exception {
        assertThat(lock.heldByCurrentThread()).isFalse();

        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        doWithLock1();
        assertThat(lock.heldByCurrentThread()).isTrue();

        doWithLock2();
        assertThat(lock.heldByCurrentThread()).isTrue();

        lock.unlock();
        assertThat(lock.heldByCurrentThread()).isFalse();
    }

    private void doWithLock1() throws Exception {
        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        TimeUnit.MILLISECONDS.sleep(600);

        lock.unlock();
    }

    private void doWithLock2() throws Exception {
        lock.lock();
        assertThat(lock.heldByCurrentThread()).isTrue();

        TimeUnit.MILLISECONDS.sleep(600);

        lock.unlock();
    }

    @AfterEach
    void after() throws IOException {
        client.close();
        server.stop();
    }
}
