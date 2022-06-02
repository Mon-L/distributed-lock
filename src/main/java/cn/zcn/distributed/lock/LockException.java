package cn.zcn.distributed.lock;

public class LockException extends RuntimeException {
    
    public LockException(String msg) {
        super(msg);
    }

    public LockException(String msg, Throwable t) {
        super(msg, t);
    }
}