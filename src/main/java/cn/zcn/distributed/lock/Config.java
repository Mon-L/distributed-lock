package cn.zcn.distributed.lock;

public class Config {

    /**
     * response timeout, millis
     */
    private int timeout = 5000;

    /**
     * 重连间隔
     */
    private long reconnectInterval = 5000;

    public int getTimeout() {
        return timeout;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }
}
