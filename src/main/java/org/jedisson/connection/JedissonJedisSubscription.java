package org.jedisson.connection;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.util.AbstractSubscription;

import redis.clients.jedis.BinaryJedisPubSub;

class JedissonJedisSubscription extends AbstractSubscription{
	private final BinaryJedisPubSub jedisPubSub;

	JedissonJedisSubscription(MessageListener listener, BinaryJedisPubSub jedisPubSub, byte[][] channels, byte[][] patterns) {
		super(listener, channels, patterns);
		this.jedisPubSub = jedisPubSub;
	}

	protected void doClose() {
		if (!getChannels().isEmpty()) {
			jedisPubSub.unsubscribe();
		}
		if (!getPatterns().isEmpty()) {
			jedisPubSub.punsubscribe();
		}
	}

	protected void doPsubscribe(byte[]... patterns) {
		jedisPubSub.psubscribe(patterns);
	}

	protected void doPUnsubscribe(boolean all, byte[]... patterns) {
		if (all) {
			jedisPubSub.punsubscribe();
		} else {
			jedisPubSub.punsubscribe(patterns);
		}
	}

	protected void doSubscribe(byte[]... channels) {
		jedisPubSub.subscribe(channels);
	}

	protected void doUnsubscribe(boolean all, byte[]... channels) {
		if (all) {
			jedisPubSub.unsubscribe();
		} else {
			jedisPubSub.unsubscribe(channels);
		}
	}
}
