package org.jedisson.api;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface IJedissonPromise<V> extends Future<V>{	
	
	public Future<V> getFuture();
		
	public IJedissonPromise<V> setSuccess(Object v);
	
	public IJedissonPromise<V> setFailure(Throwable cause);
	
	public boolean isSuccess();
	 	
	public IJedissonPromise<V> await() throws InterruptedException;
	
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException;
	
	public IJedissonPromise<V> onComplete(IPromiseListener<IJedissonPromise> listener);
	
	public IJedissonPromise<V> onSuccess(IPromiseListener<IJedissonPromise> listener);
	
	public IJedissonPromise<V> onFailure(IPromiseListener<IJedissonPromise> listener);
	
	public static interface IPromiseListener<T extends IJedissonPromise>{
		public IJedissonPromise apply(T promise);
	}
}
