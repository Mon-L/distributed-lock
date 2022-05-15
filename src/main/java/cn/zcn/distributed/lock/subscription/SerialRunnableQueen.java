package cn.zcn.distributed.lock.subscription;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 串行执行的任务队列。必须等上一个任务完成，才能执行下一个任务。
 */
public class SerialRunnableQueen {

    private final AtomicBoolean canRun = new AtomicBoolean(true);

    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

    public void add(Runnable runnable) {
        tasks.add(runnable);
        tryRun();
    }

    private void tryRun() {
        if (canRun.compareAndSet(true, false)) {
            Runnable task = tasks.poll();

            if (task == null) {
                canRun.set(true);
                return;
            }

            task.run();
        }
    }

    public int getQueenSize() {
        return tasks.size();
    }

    public void runNext() {
        canRun.set(true);
        tryRun();
    }
}
