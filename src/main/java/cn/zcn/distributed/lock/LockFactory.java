package cn.zcn.distributed.lock;

/**
 * 分布式锁工厂
 */
public interface LockFactory {

    /**
     * 注册JVM关闭钩子
     */
    default void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(LockFactory.this::shutdown));
    }

    /**
     * 初始化工厂
     */
    void start();

    /**
     * 获取分布式非公平锁
     *
     * @param name 锁名
     * @return 非公平锁
     */
    Lock getLock(String name);

    /**
     * 获取分布式公平锁
     *
     * @param name 锁名
     * @return 公平锁
     */
    Lock getFairLock(String name);

    /**
     * 关闭工厂
     */
    void shutdown();
}
