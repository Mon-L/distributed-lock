package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.redis.RedisSubscription;
import cn.zcn.distributed.lock.redis.RedisSubscriptionListener;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

class LettuceSubscription implements RedisSubscription {

    private final LettucePubSubListener lettucePubSubListener;
    private final RedisPubSubCommands<byte[], byte[]> pubSubCommands;
    private final StatefulRedisPubSubConnection<byte[], byte[]> connection;

    public LettuceSubscription(StatefulRedisPubSubConnection<byte[], byte[]> connection, RedisSubscriptionListener listener) {
        this.connection = connection;
        this.lettucePubSubListener = new LettucePubSubListener(listener);
        this.connection.addListener(lettucePubSubListener);
        this.pubSubCommands = this.connection.sync();
    }

    @Override
    public void subscribe(byte[]... channels) {
        pubSubCommands.subscribe(channels);
    }

    @Override
    public void unsubscribe(byte[]... channels) {
        pubSubCommands.unsubscribe(channels);
    }

    @Override
    public long getSubscribedChannels() {
        return lettucePubSubListener.subscribedChannels;
    }

    @Override
    public void close() {
        pubSubCommands.unsubscribe();
        connection.removeListener(lettucePubSubListener);
        connection.close();
    }

    private static class LettucePubSubListener extends RedisPubSubAdapter<byte[], byte[]> {

        private volatile long subscribedChannels;
        private final RedisSubscriptionListener listener;

        private LettucePubSubListener(RedisSubscriptionListener listener) {
            this.listener = listener;
        }

        @Override
        public void message(byte[] channel, byte[] message) {
            listener.onMessage(channel, message);
        }

        @Override
        public void subscribed(byte[] channel, long subscribedChannels) {
            this.subscribedChannels = subscribedChannels;
            listener.onSubscribe(channel, subscribedChannels);
        }

        @Override
        public void unsubscribed(byte[] channel, long subscribedChannels) {
            this.subscribedChannels = subscribedChannels;
            listener.onUnsubscribe(channel, subscribedChannels);
        }
    }
}
