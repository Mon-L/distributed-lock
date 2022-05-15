package cn.zcn.distributed.lock.exception;

public class TimeoutException extends RuntimeException {
    public TimeoutException(String msg) {
        super(msg);
    }

    public TimeoutException(String msg, Throwable t) {
        super(msg, t);
    }
}
