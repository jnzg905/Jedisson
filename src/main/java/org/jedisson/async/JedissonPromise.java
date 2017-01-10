package org.jedisson.async;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.jedisson.api.IJedissonPromise;
import org.jedisson.api.IJedissonSerializer;
import org.springframework.util.Assert;

public class JedissonPromise<V> implements IJedissonPromise<V>{

	private static final AtomicReferenceFieldUpdater<JedissonPromise, Object> RESULT_UPDATER = 
			AtomicReferenceFieldUpdater.newUpdater(JedissonPromise.class,Object.class, "result");
	
	private static final Object SUCCESS = new Object();
		
	private static final CancellationException CANCEL = new CancellationException();
	
	private volatile Object result;
	
	private List<IPromiseListener> listeners = new ArrayList<>();
	
	private short waiters;
	
	private IJedissonSerializer<?> serializer;
	
	public JedissonPromise(IJedissonSerializer<?> serializer){
		this.serializer = serializer;
	}
	
	@Override
	public V get() throws InterruptedException, ExecutionException {
		await();
		Object result = this.result;
		if(result == SUCCESS){
			return null;
		}
		
        if (result instanceof Throwable) {
            throw new ExecutionException((Throwable)result);
        }
        return (V) result;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		if(!await(timeout,unit)){
			throw new TimeoutException("Futures timed out after [" + unit.toMillis(timeout) + "ms]");	
		}
		
		Object result = this.result;
		if(result == SUCCESS){
			return null;
		}
	    if (result instanceof Throwable) {
	    	throw new ExecutionException((Throwable)result);
	    }
	    return (V) result;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (RESULT_UPDATER.compareAndSet(this, null, CANCEL)) {
			checkNotifyWaiters();
			notifyListeners();
			return true;
		}
		return false;
	}

	@Override
	public boolean isSuccess() {
		return result != null && !(result instanceof Throwable); 
	}

	@Override
	public boolean isCancelled() {
		 return result == CANCEL;
	}

	@Override
	public boolean isDone() {
		return result != null;
	}

	@Override
	public IJedissonPromise<V> await() throws InterruptedException{
		if(isDone()){
			return this;
		}
		
		if(Thread.interrupted()){
			throw new InterruptedException();
		}
		
		synchronized(this){
			while(!isDone()){
				incWaiters();
				try{
					wait();
				}finally{
					decWaiters();
				}
			}
		}
		return this;
	}
	
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException{
		if (isDone()) {
			return true;
		}
		
		if (timeout <= 0) {
			return isDone();
		}
		
		if (Thread.interrupted()) {
			throw new InterruptedException(toString());
		}

		long startTime = System.nanoTime();
		long waitTime = timeout;
		while (true) {
			synchronized (this) {
				if (isDone()) {
					return true;
				}
				incWaiters();
				try {
					wait(waitTime / 1000000, (int) (waitTime % 1000000));
				} finally {
					decWaiters();
				}

				if (isDone()) {
					return true;
				} else {
					waitTime = timeout - (System.nanoTime() - startTime);
					if (waitTime <= 0) {
						return isDone();
					}
				}
			}
		}
	}
	
	private void incWaiters() {
		if (waiters == Short.MAX_VALUE) {
			throw new IllegalStateException("too many waiters: " + this);
		}
		++waiters;
	}
	
	private void decWaiters() {
		--waiters;
	}

	private boolean setValue(Object result) {
		if (RESULT_UPDATER.compareAndSet(this, null, result == null ? SUCCESS : (result instanceof byte[]) ? serializer.deserialize((byte[]) result) : result)) {
			checkNotifyWaiters();
            return true;
        }
        return false;
    }
	
	private void notifyListeners(){
		Iterator<IPromiseListener> iter = listeners.iterator();
		IJedissonPromise promise = this;
		while(iter.hasNext()){
			promise = iter.next().apply(promise);
			iter.remove();
		}
	}
	
	private synchronized void checkNotifyWaiters() {
		if (waiters > 0) {
			notifyAll();
		}
	}

	private boolean done(Object result) {
		if(setValue(result)){
			notifyListeners();
			return true;
		}
		return false;
	}

	@Override
	public Future<V> getFuture() {
		return this;
	}

	@Override
	public IJedissonPromise<V> setSuccess(Object v) {
		if(!done(v)){
			throw new IllegalStateException("Already completed future: " + this);
		}
		return this;
	}

	@Override
	public IJedissonPromise<V> setFailure(Throwable cause) {
		if(!done(cause)){
			throw new IllegalStateException("Already completed future: " + this);
		}
		return this;
	}

	@Override
	public IJedissonPromise<V> onComplete(IPromiseListener<IJedissonPromise> listener) {
		 if (isDone()) {
	            return listener.apply(this);
	        }
	        synchronized (this) {
	            if (isDone()) {
	                return listener.apply(this);
	            }
	            listeners.add(listener);
	        }
	        return this;
	}

	@Override
	public IJedissonPromise<V> onSuccess(IPromiseListener<IJedissonPromise> listener) {
		 return onComplete(new IPromiseListener<IJedissonPromise>() {
	            @Override
	            public IJedissonPromise<V> apply(final IJedissonPromise promise) {
	                if (promise.isSuccess()) {
	                    return listener.apply(promise);
	                }
	                return promise;
	            }
	        });
	}

	@Override
	public IJedissonPromise<V> onFailure(IPromiseListener listener) {
		return onComplete(new IPromiseListener<IJedissonPromise>() {
            @Override
            public IJedissonPromise<V> apply(final IJedissonPromise promise) {
                if (!promise.isSuccess()) {
                    return listener.apply(promise);
                }
                return promise;
            }
        });
	} 
	
	
}
