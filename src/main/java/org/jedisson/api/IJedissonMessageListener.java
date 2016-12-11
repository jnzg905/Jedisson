package org.jedisson.api;

public interface IJedissonMessageListener<T> {

	public void onMessage(T t);
	
	public IJedissonSerializer<T> getSerializer();
}
