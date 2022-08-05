package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ZookeeperReadWriteLockTest extends BaseZookeeperTest {

    private ZookeeperLock readLock;
    private ZookeeperLock writeLock;
    private CuratorFramework client;

    @BeforeEach
    void before() {
        Timing timing = new Timing();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(200, 3);
        client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), retryPolicy);
        client.start();

        ZookeeperReadWriteLock lock = new ZookeeperReadWriteLock("/test-lock", client);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    void checkInitialState() {
        assertThat(readLock.heldByCurrentThread()).isFalse();
        assertThat(writeLock.heldByCurrentThread()).isFalse();
    }

    @Test
    void testRead() throws Exception {
        checkInitialState();

        int threadNum = 10;
        ExecutorCompletionService<Object> service = new ExecutorCompletionService<>(Executors.newCachedThreadPool());
        for (int i = 0; i < threadNum; i++) {
            service.submit(() -> {
                readLock.lock();
                assertThat(readLock.heldByCurrentThread()).isTrue();
                return new Object();
            });
        }

        for (int i = 0; i < threadNum; i++) {
            assertThat(service.take().get(3, TimeUnit.SECONDS)).isNotNull();
        }
    }

    @Test
    void testReadButHasWriteLock() throws Exception {
        checkInitialState();

        int threadNum = 10;
        Semaphore semaphore = new Semaphore(threadNum);
        CountDownLatch isDone = new CountDownLatch(threadNum);
        ExecutorService service = Executors.newCachedThreadPool();
        List<Object> objects = Collections.synchronizedList(new ArrayList<>());

        writeLock.lock();

        for (int i = 0; i < threadNum; i++) {
            service.submit(() -> {
                semaphore.acquire();

                readLock.lock();
                objects.add(null);
                isDone.countDown();

                return null;
            });
        }

        while (semaphore.availablePermits() > 0) {
            TimeUnit.MILLISECONDS.sleep(200);
        }
        TimeUnit.MILLISECONDS.sleep(200);

        assertThat(objects.size()).isEqualTo(0);
        writeLock.unlock();

        isDone.await(3, TimeUnit.SECONDS);
        assertThat(objects.size()).isEqualTo(10);
    }

    @Test
    void testWriteButHasReadLock() throws Exception {
        int threadNum = 10;
        Semaphore semaphore = new Semaphore(0);
        ExecutorService service = Executors.newCachedThreadPool();

        //acquire 10 read lock
        for (int i = 0; i < threadNum; i++) {
            service.submit(() -> {
                readLock.lock();
                semaphore.acquire();
                readLock.unlock();
                return null;
            });
        }

        //acquire write lock
        Future<Boolean> hasWriteLock = service.submit(() -> {
            assertThat(writeLock.heldByCurrentThread()).isFalse();
            writeLock.lock();
            assertThat(writeLock.heldByCurrentThread()).isTrue();
            return true;
        });

        //release all read lock
        for (int i = 0; i < threadNum; i++) {
            semaphore.release();
        }

        assertThat(hasWriteLock.get(3, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void testReadIfHeldWriteLock() {
        ExecutorService service = Executors.newSingleThreadExecutor();
        Future<?> f = service.submit(() -> {
            checkInitialState();

            try {
                //acquire write lock
                writeLock.lock();

                //acquire read lock
                readLock.lock();
                assertThat(writeLock.heldByCurrentThread()).isTrue();
                assertThat(readLock.heldByCurrentThread()).isTrue();

                //release write lock
                writeLock.unlock();
                assertThat(writeLock.heldByCurrentThread()).isFalse();
                assertThat(readLock.heldByCurrentThread()).isTrue();

                //release read lock
                readLock.unlock();
                assertThat(writeLock.heldByCurrentThread()).isFalse();
                assertThat(readLock.heldByCurrentThread()).isFalse();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        try {
            f.get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            service.shutdownNow();
        }
    }

    @AfterEach
    void after() {
        CloseableUtils.closeQuietly(client);
    }
}
