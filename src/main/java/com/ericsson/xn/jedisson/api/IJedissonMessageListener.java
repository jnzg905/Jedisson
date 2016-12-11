package com.ericsson.xn.jedisson.api;

public interface IJedissonMessageListener<T> {

	public void onMessage(T t);
	
	public IJedissonSerializer<T> getSerializer();
}
