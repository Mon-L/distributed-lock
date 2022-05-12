package cn.zcn.distributed.lock;

import java.util.concurrent.TimeUnit;

public interface ZLock {

    /**
     * 申请一个锁，锁的持续时间为 {@code duration}。阻塞当前线程，直到锁申请成功，可被中断
     *
     * @param duration         期望申请的锁的持续时间
     * @param durationTimeUnit 时间单位
     * @throws InterruptedException 中断异常
     */
    void lock(long duration, TimeUnit durationTimeUnit) throws InterruptedException;

    /**
     * 申请一个锁，锁的持续时间为 {@code duration}。阻塞当前线程，直到锁申请成功，最长阻塞等待时间为 {@code waitTime}，可被中断
     *
     * @param waitTime         申请锁的最长等待时间
     * @param waitTimeUnit     申请锁的最长等待时间的单位
     * @param duration         期望申请的锁的持续时间
     * @param durationTimeUnit 锁的持续时间的单位
     * @return true, 在指定时间内申请锁成功；false，申请锁失败
     * @throws InterruptedException 中断异常
     */
    boolean tryLock(long waitTime, TimeUnit waitTimeUnit, long duration, TimeUnit durationTimeUnit) throws InterruptedException;

    /**
     * 释放获得的锁
     */
    void unlock();

    /**
     * 续锁
     */
    void renew();

    /**
     * 判断锁是否被当前线程持有
     *
     * @return true, 当前线程持有该锁；false，当前线程不持有该锁
     */
    boolean isHeldByCurrentThread();
}
