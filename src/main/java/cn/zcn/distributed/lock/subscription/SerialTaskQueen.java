package cn.zcn.distributed.lock.subscription;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class SerialTaskQueen {

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

    public void add(Runnable runnable) {
        tasks.add(runnable);
        tryRun();
    }

    private void tryRun() {
        if (isRunning.compareAndSet(false, true)) {
            Runnable task = tasks.poll();
            if (task != null) {
                task.run();
            }
        }
    }

    public void runNext() {
        isRunning.set(true);
        tryRun();
    }
}
