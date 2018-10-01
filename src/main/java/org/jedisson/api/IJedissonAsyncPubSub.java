package org.jedisson.api;

import java.util.concurrent.CompletableFuture;

public interface IJedissonAsyncPubSub {

	public <T> void subscribe(final String topic, IJedissonMessageListener<T> messageListener);
	
	public void unsubscribe(final String topic, IJedissonMessageListener listener);
	
	public <T> CompletableFuture<Long> publish(final String tpoic, T message);
}
