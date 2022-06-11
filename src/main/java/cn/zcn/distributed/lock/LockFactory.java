package cn.zcn.distributed.lock;

public interface LockFactory {

    default void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(LockFactory.this::shutdown));
    }

    void start();

    Lock getLock(String name);

    Lock getFairLock(String name);

    void shutdown();
}
