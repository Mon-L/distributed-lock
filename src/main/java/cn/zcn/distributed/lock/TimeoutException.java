package cn.zcn.distributed.lock;

public class TimeoutException extends RuntimeException {

    public TimeoutException(String msg) {
        super(msg);
    }
}
