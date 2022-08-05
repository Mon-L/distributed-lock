package cn.zcn.distributed.lock.redis;

public class LockException extends RuntimeException {
    
    public LockException(String msg, Throwable t) {
        super(msg, t);
    }
}