package cn.zcn.distributed.lock.redis.subscription;

import cn.zcn.distributed.lock.redis.subscription.SerialRunnableQueen;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

public class SerialRunnableQueenTest {

    private SerialRunnableQueen queen;

    @BeforeEach
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
        assertThat(queen.getQueenSize()).isEqualTo(0);
        assertThat(count[0]).isEqualTo(10);

        queen.add(() -> count[0] += 1);
        assertThat(queen.getQueenSize()).isEqualTo(0);
        assertThat(count[0]).isEqualTo(11);

        queen.add(() -> count[0] += 1);
        assertThat(queen.getQueenSize()).isEqualTo(1);
        assertThat(count[0]).isEqualTo(11);

        queen.runNext();
        assertThat(queen.getQueenSize()).isEqualTo(0);
        assertThat(count[0]).isEqualTo(12);
    }

    @Test
    public void testSerialAdd2() {
        final int[] count = {0};
        for (int i = 0; i < 10; i++) {
            queen.add(() -> count[0] += 1);
        }
        assertThat(queen.getQueenSize()).isEqualTo(9);
        assertThat(count[0]).isEqualTo(1);

        for (int i = 0; i < 9; i++) {
            queen.runNext();
        }
        assertThat(queen.getQueenSize()).isEqualTo(0);
        assertThat(count[0]).isEqualTo(10);

        queen.add(() -> count[0] += 1);
        assertThat(queen.getQueenSize()).isEqualTo(1);
        assertThat(count[0]).isEqualTo(10);

        queen.runNext();
        assertThat(queen.getQueenSize()).isEqualTo(0);
        assertThat(count[0]).isEqualTo(11);
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
        assertThat(queen.getQueenSize()).isEqualTo(19);
        assertThat(count[0]).isEqualTo(1);

        for (int i = 0; i < 19; i++) {
            queen.runNext();
            assertThat(queen.getQueenSize()).isEqualTo(19 - i - 1);
        }

        queen.add(() -> count[0] += 1);
        assertThat(queen.getQueenSize()).isEqualTo(1);
        assertThat(count[0]).isEqualTo(20);

        queen.runNext();
        assertThat(queen.getQueenSize()).isEqualTo(0);
    }
}
