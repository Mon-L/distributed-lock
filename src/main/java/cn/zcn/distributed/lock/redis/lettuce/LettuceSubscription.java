package cn.zcn.distributed.lock.redis.lettuce;

import cn.zcn.distributed.lock.redis.subscription.AbstractRedisSubscription;
import cn.zcn.distributed.lock.redis.subscription.RedisSubscriptionListener;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;

class LettuceSubscription extends AbstractRedisSubscription {

    private LettucePubSubAdapter lettucePubSubAdapter;
    private final RedisPubSubAsyncCommands<byte[], byte[]> pubSubCommands;
    private final StatefulRedisPubSubConnection<byte[], byte[]> connection;

    public LettuceSubscription(StatefulRedisPubSubConnection<byte[], byte[]> connection) {
        this.connection = connection;
        this.pubSubCommands = this.connection.async();
    }

    @Override
    public void doSubscribe(RedisSubscriptionListener listener, byte[]... channels) {
        this.lettucePubSubAdapter = new LettucePubSubAdapter(listener);
        this.connection.addListener(lettucePubSubAdapter);

        subscribe(channels);
    }

    @Override
    public void doSubscribe(byte[]... channels) {
        pubSubCommands.subscribe(channels);
    }

    @Override
    public void doUnsubscribe(byte[]... channels) {
        pubSubCommands.unsubscribe(channels);
    }

    @Override
    public long getSubscribedChannels() {
        return isAlive() ? lettucePubSubAdapter.subscribedChannels : 0;
    }

    @Override
    public boolean isAlive() {
        return connection.isOpen();
    }

    @Override
    public void close() {
        if (isAlive()) {
            pubSubCommands.unsubscribe();
            connection.removeListener(lettucePubSubAdapter);
            connection.close();
        }
    }

    private static class LettucePubSubAdapter extends RedisPubSubAdapter<byte[], byte[]> {

        private volatile long subscribedChannels;
        private final RedisSubscriptionListener listener;

        private LettucePubSubAdapter(RedisSubscriptionListener listener) {
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
