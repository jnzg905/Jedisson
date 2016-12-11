package org.jedisson.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.lock.JedissonLock;
import org.junit.Test;

public class JedissonLockTest extends BaseTest{

	private int index;
	@Test
	public void testJedissonLockLock() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(100);
		
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		ExecutorService executor = Executors.newFixedThreadPool(1);
		
		for(int i = 0; i < 100; i++){
			final JedissonLock lock = jedisson.getLock("my_lock");
			executor.submit(new Runnable(){

				@Override
				public void run() {
					lock.lock();
					index = index + 1;
					System.out.println(index);
					lock.unlock();
					latch.countDown();
				}
			});
		}
		latch.await();
	}
	
	@Test
	public void testNativeLockRate() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(100);
		
		ExecutorService executor = Executors.newFixedThreadPool(4);
		for(int i = 0; i < 100; i++){
			final Lock lock = new ReentrantLock();
			executor.submit(new Runnable(){

				@Override
				public void run() {
					
					long start = System.currentTimeMillis();
					for(int i = 0; i < 10000; i++){
						lock.lock();
						index = index + 1;
						lock.unlock();
					}
					System.out.println(Thread.currentThread().getId() + ":" + (System.currentTimeMillis() - start));
					latch.countDown();
				}
				
			});
		}
		latch.await();
	}
	
	@Test
	public void testJedissonLockRate() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(4);
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		ExecutorService executor = Executors.newFixedThreadPool(4);
		
		for(int i = 0; i < 4; i++){
			final JedissonLock lock = jedisson.getLock("my_lock");
			executor.submit(new Runnable(){

				@Override
				public void run() {
					long start = System.currentTimeMillis();
					for(int i = 0; i < 1000; i++){
						lock.lock();
						lock.unlock();
					}
					System.out.println(Thread.currentThread().getId() + ":" + (System.currentTimeMillis() - start));
					latch.countDown();
				}
			});
	
		}
		latch.await();
	}
}
