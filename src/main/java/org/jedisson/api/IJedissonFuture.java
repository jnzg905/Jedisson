package org.jedisson.api;

import java.util.concurrent.TimeUnit;

import org.jedisson.common.JedissonException;

public interface IJedissonFuture<V> {

	public V get() throws InterruptedException;
	
	public V get(long timeout, TimeUnit unit) throws InterruptedException;
	
	public boolean cancel() throws InterruptedException;
	
	public boolean isCancel();
	
	public boolean done(V v);
	
	public boolean isDone();
	
	public void listen(IJedissonClosure<? super IJedissonFuture<V>> listener);
}
