package org.jedisson.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.jedisson.api.IJedissonClosure;
import org.jedisson.api.IJedissonFuture;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonException;
import org.springframework.util.Assert;

public class JedissonFuture<V> implements IJedissonFuture<V>{

	private static final AtomicReferenceFieldUpdater<JedissonFuture, Object> RESULT_UPDATER = 
			AtomicReferenceFieldUpdater.newUpdater(JedissonFuture.class,Object.class, "result");
	
	private static final Object SUCCESS = new Object();
	
	private static final CancellationException CANCEL = new CancellationException();
	
	private volatile Object result;
	
	private List<IJedissonClosure> listeners = new ArrayList<>();
	
	private short waiters;
	
	private IJedissonSerializer<?> serializer;
	
	public JedissonFuture(IJedissonSerializer<?> serializer){
		this.serializer = serializer;
	}
	
	@Override
	public V get() throws InterruptedException {
		await();
		Object result = this.result;
        if (result instanceof Throwable || result == SUCCESS) {
            return null;
        }
        return (V) result;
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException {
		if(await(timeout,unit)){
			Object result = this.result;
	        if (result instanceof Throwable || result == SUCCESS) {
	            return null;
	        }
	        return (V) result;
		}
		return null;
	}

	@Override
	public boolean cancel() throws JedissonException {
		if (RESULT_UPDATER.compareAndSet(this, null, CANCEL)) {
			checkNotifyWaiters();
			notifyListeners();
			return true;
		}
		return false;
	}

	@Override
	public boolean isCancel() {
		 return result == CANCEL;
	}

	@Override
	public boolean isDone() {
		return result != null && !(result instanceof Throwable);
	}

	@Override
	public void listen(IJedissonClosure<? super IJedissonFuture<V>> listener) {
		Assert.notNull(listener);
		listeners.add(listener);
	}

	private void await() throws InterruptedException{
		if(isDone()){
			return;
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
	}
	
	private void awaitUninterruptibly(){
		if(isDone()){
			return;
		}
		
		boolean interrupted = false;
		synchronized(this){
			while(!isDone()){
				incWaiters();
				try{
					wait();
				}catch(InterruptedException e){
					interrupted = true;
				}finally{
					decWaiters();
				}
			}
		}
		
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}
	
	private boolean await(long timeout, TimeUnit unit) throws InterruptedException{
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
		for(IJedissonClosure listener : listeners){
			listener.apply(this);
		}
	}
	private synchronized void checkNotifyWaiters() {
		if (waiters > 0) {
			notifyAll();
		}
	}
	 
	@Override
	public boolean done(V v) {
		if(setValue(v)){
			notifyListeners();
			return true;
		}
		return false;
	} 
}
