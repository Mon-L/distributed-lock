package cn.zcn.distributed.lock.test.redis;

import java.util.LinkedList;

public enum ShutdownQueue {
    INSTANCE;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Runnable runnable;
                while ((runnable = INSTANCE.runnables.pollLast()) != null) {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    private final LinkedList<Runnable> runnables = new LinkedList<>();

    public static void register(Runnable runnable) {
        INSTANCE.runnables.add(runnable);
    }
}