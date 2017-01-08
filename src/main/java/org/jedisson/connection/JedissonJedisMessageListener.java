package org.jedisson.connection;

import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.util.Assert;

import redis.clients.jedis.BinaryJedisPubSub;

class JedissonJedisMessageListener extends BinaryJedisPubSub{
	private final MessageListener listener;

	JedissonJedisMessageListener(MessageListener listener) {
		Assert.notNull(listener, "message listener is required");
		this.listener = listener;
	}

	public void onMessage(byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), null);
	}

	public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
		listener.onMessage(new DefaultMessage(channel, message), pattern);
	}

	public void onPSubscribe(byte[] pattern, int subscribedChannels) {
		// no-op
	}

	public void onPUnsubscribe(byte[] pattern, int subscribedChannels) {
		// no-op
	}

	public void onSubscribe(byte[] channel, int subscribedChannels) {
		// no-op
	}

	public void onUnsubscribe(byte[] channel, int subscribedChannels) {
		// no-op
	}
}
