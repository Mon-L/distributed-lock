package cn.zcn.distributed.lock.exception;

public class LockException extends RuntimeException {
    
    public LockException(String msg) {
        super(msg);
    }

    public LockException(String msg, Throwable t) {
        super(msg, t);
    }
}