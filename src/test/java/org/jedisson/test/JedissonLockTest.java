package org.jedisson.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.lock.JedissonLock;
import org.jedisson.lock.JedissonReentrantLock;
import org.junit.Test;

public class JedissonLockTest extends BaseTest{

	private int index;
	@Test
	public void testJedissonLockLock() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(100);
		
		IJedisson jedisson = Jedisson.getJedisson();
		ExecutorService executor = Executors.newFixedThreadPool(10);
		
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
		IJedisson jedisson = Jedisson.getJedisson();
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
	
	@Test
	public void testJedissonReentrantLock() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(100);
		IJedisson jedisson = Jedisson.getJedisson();
		ExecutorService executor = Executors.newFixedThreadPool(10);
		for(int i = 0; i < 100; i++){
			final JedissonReentrantLock lock = jedisson.getReentrantLock("my_lock");
			executor.submit(new Runnable(){

				@Override
				public void run() {
					for(int i = 0; i <10; i++){
						lock.lock();
						index = index + 1;
						System.out.println(index);
					}
					for(int i = 0; i < 10; i++){
						lock.unlock();	
					}
					latch.countDown();
				}
			});
		}
		latch.await();
	}
	
	@Test
	public void testStringTruncate(){
		String str = "Alarm ResourceID= .1.3.6.1.4.1.193.169.8.1.14.22.71.69.84.95.82.83.65.83.85.66.83.49.53.95.67.79.85.78.84.69.82.83";
		
		String subStr = str.length() > 255 ? str.substring(0,255) : str;
		System.out.println(subStr);
	}
}
