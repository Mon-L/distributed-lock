package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.subscription.SerialRunnableQueen;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class SerialRunnableQueenTest {

    private SerialRunnableQueen queen;

    @Before
    public void before() {
        queen = new SerialRunnableQueen();
    }

    @Test
    public void testSerialAdd() {
        final int[] count = {0};
        for (int i = 0; i < 10; i++) {
            queen.add(() -> {
                count[0] += 1;
                queen.runNext();
            });
        }
        Assert.assertEquals(0, queen.getQueenSize());
        Assert.assertEquals(10, count[0]);

        queen.add(() -> count[0] += 1);
        Assert.assertEquals(0, queen.getQueenSize());
        Assert.assertEquals(11, count[0]);

        queen.add(() -> count[0] += 1);
        Assert.assertEquals(1, queen.getQueenSize());
        Assert.assertEquals(11, count[0]);

        queen.runNext();
        Assert.assertEquals(0, queen.getQueenSize());
        Assert.assertEquals(12, count[0]);
    }

    @Test
    public void testSerialAdd2() {
        final int[] count = {0};
        for (int i = 0; i < 10; i++) {
            queen.add(() -> count[0] += 1);
        }
        Assert.assertEquals(9, queen.getQueenSize());
        Assert.assertEquals(1, count[0]);

        for (int i = 0; i < 9; i++) {
            queen.runNext();
        }
        Assert.assertEquals(0, queen.getQueenSize());
        Assert.assertEquals(10, count[0]);

        queen.add(() -> count[0] += 1);
        Assert.assertEquals(1, queen.getQueenSize());
        Assert.assertEquals(10, count[0]);

        queen.runNext();
        Assert.assertEquals(0, queen.getQueenSize());
        Assert.assertEquals(11, count[0]);
    }

    @Test
    public void testConcurrentAdd() throws InterruptedException {
        final int[] count = {0};
        CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                queen.add(() -> count[0] += 1);
            }

            latch.countDown();
        }).start();

        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                queen.add(() -> count[0] += 1);
            }
            latch.countDown();
        }).start();

        latch.await();
        Assert.assertEquals(19, queen.getQueenSize());
        Assert.assertEquals(1, count[0]);

        for (int i = 0; i < 19; i++) {
            queen.runNext();
            Assert.assertEquals(19 - i - 1, queen.getQueenSize());
        }

        queen.add(() -> count[0] += 1);
        Assert.assertEquals(1, queen.getQueenSize());
        Assert.assertEquals(20, count[0]);

        queen.runNext();
        Assert.assertEquals(0, queen.getQueenSize());
    }
}
