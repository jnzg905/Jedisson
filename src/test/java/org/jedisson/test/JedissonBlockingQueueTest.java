package org.jedisson.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonBlockingQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSON;

@PropertySource("file:config/jedisson.properties")
@RunWith(SpringRunner.class)
@SpringBootTest(classes=JedissonBlockingQueueTest.class)
@SpringBootApplication(scanBasePackages="org.jedisson")
public class JedissonBlockingQueueTest{
	@Before
	public void testBegin() throws InterruptedException{
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonBlockingQueue<TestObject> queue = jedisson.getBlockingQueue("myBlockingQueue", TestObject.class);
		for(int j = 0; j < 100; j++){
			TestObject test = new TestObject();
			test.setName("test" + j);
			test.setAge(j);
			test.getFriends().add("friends" + j);
			queue.put(test);
		}
	}
	
	@After
	public void testEnd(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myBlockingQueue", TestObject.class);
		queue.clear();
	}
	
	@Test
	public void testBlockingQueuePutAndTake() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(11);
		ExecutorService consumers = Executors.newFixedThreadPool(1);
		
		consumers.submit(new Runnable(){

			@Override
			public void run() {
				IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myQueue", TestObject.class);
				while(true){
					try {
						TestObject test = queue.poll(10 * 1000, TimeUnit.MILLISECONDS);
						if(test == null){
							latch.countDown();
							break;
						}
						System.out.println(JSON.toJSONString(test));
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}
			
		});
		
		ExecutorService providers = Executors.newFixedThreadPool(10);
		
		
		for(int i = 0; i < 10; i++){
			providers.submit(new Runnable(){

				@Override
				public void run() {
					try{
						IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myQueue", TestObject.class);
						for(int j = 0; j < 100; j++){
							TestObject test = new TestObject();
							test.setName(Thread.currentThread().getId() + ":" + "blockingqueue" + j);
							test.setAge(j);
							test.getFriends().add("friends" + j);
							queue.put(test);
						}
						latch.countDown();
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			});
		}
		
		latch.await();
	}
	
	@Test
	public void testBlockingQueuePut() throws InterruptedException{
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myQueue", TestObject.class);
		for(int j = 0; j < 100; j++){
			TestObject test = new TestObject();
			test.setName(Thread.currentThread().getId() + ":" + "blockingqueue" + j);
			test.setAge(j);
			test.getFriends().add("friends" + j);
			queue.put(test);
		}
	}
	
	@Test
	public void testBlockingQueuePoll(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myQueue", TestObject.class);
		
		while(true){
			TestObject test = queue.poll();
			if(test == null){
				break;
			}
			System.out.println(JSON.toJSONString(test));
		}
	}
	
	@Test
	public void testBlockingQueueRemove(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myBlockingQueue", TestObject.class);
		TestObject test = queue.remove();
		Assert.assertEquals("test0", test.getName());
		Assert.assertEquals(99, queue.size());
	}
	
	@Test
	public void testBlockingQueueRemoveObject(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myBlockingQueue", TestObject.class);
		TestObject test = queue.peek();
		Assert.assertEquals(true,queue.remove(test));
		Assert.assertEquals(99, queue.size());
		Assert.assertEquals("test1", queue.peek().getName());
	}
	
	@Test
	public void testBlockingQueueContains(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myBlockingQueue", TestObject.class);
		TestObject test = new TestObject();
		test.setName("test" + 2);
		test.setAge(2);
		test.getFriends().add("friends" + 2);
		Assert.assertEquals(true,queue.contains(test));
		test.setName("test" + 2);
		test.setAge(3);
		test.getFriends().add("friends" + 2);
		Assert.assertEquals(false,queue.contains(test));
	}
	
	@Test
	public void testBlockingQueueDrainTo(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myBlockingQueue", TestObject.class);
		
		List<TestObject> list = new ArrayList<>();
		
		int num = queue.drainTo(list);
		Assert.assertEquals(100, num);
		
		
		for(int i = 0; i < num; i++){
			Assert.assertEquals("test" + i, list.get(i).getName());
		}
	}
	
	@Test
	public void testBlockingQueueDrainTo2(){
		IJedissonBlockingQueue<TestObject> queue = Jedisson.getJedisson().getBlockingQueue("myBlockingQueue", TestObject.class);
		
		List<TestObject> list = new ArrayList<>();
		
		int num = queue.drainTo(list,5);
		Assert.assertEquals(5, num);
		
		
		for(int i = 0; i < num; i++){
			Assert.assertEquals("test" + i, list.get(i).getName());
		}
		
		int j = 0;
		while(true){
			TestObject test = queue.poll();
			if(test == null){
				break;
			}
			Assert.assertEquals("test" + (j + 5), test.getName());
			j++;
		}
	}
}
