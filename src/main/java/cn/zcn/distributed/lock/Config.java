package cn.zcn.distributed.lock;

public class Config {

    public static Config DEFAULT_CONFIG = new Config.Builder().build();

    /**
     * response timeout, millis
     */
    private final int timeout;

    /**
     * 重连间隔
     */
    private final long reconnectInterval;

    private Config(Builder builder) {
        this.timeout = builder.timeout;
        this.reconnectInterval = builder.reconnectInterval;
    }

    public int getTimeout() {
        return timeout;
    }

    public long getReconnectInterval() {
        return reconnectInterval;
    }

    public static class Builder {
        private int timeout = 5000;
        private long reconnectInterval = 5000;

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder reconnectInterval(long reconnectInterval) {
            this.reconnectInterval = reconnectInterval;
            return this;
        }

        public Config build() {
            return new Config(this);
        }
    }
}
