package org.jedisson.api;

public interface IJedissonPubSub {

	public <T> void subscribe(final String topic, IJedissonMessageListener<T> messageListener);
	
	public void unsubscribe(final String topic, IJedissonMessageListener listener);
	
	public <T> void publish(final String tpoic, T message);
}
