package org.jedisson.test;

import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonMessageListener;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

public class JedissonPubSubTest extends BaseTest{

	@Test
	public void testSubscribe(){
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonPubSub pubsub = jedisson.getPubSub("my_pubsub", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		IJedissonMessageListener<TestObject> listener = new IJedissonMessageListener<TestObject>(){

			@Override
			public void onMessage(TestObject t) {
				System.out.println(JSON.toJSONString(t));
				t.setName("subscribe_test");
			}

			@Override
			public IJedissonSerializer<TestObject> getSerializer() {
				// TODO Auto-generated method stub
				return new JedissonFastJsonSerializer<TestObject>(TestObject.class);
			}
			
		};
		
		pubsub.subscribe("topic_test", listener);
		
		TestObject test = new TestObject();
		test.setName("publish_test");
		pubsub.publish("topic_test", test);
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testMultiSubscribe() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(1000);
		
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonPubSub pubsub = jedisson.getPubSub("my_pubsub", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0; i < 1000; i++){
			final int id = i;
			IJedissonMessageListener<TestObject> listener = new IJedissonMessageListener<TestObject>(){

				@Override
				public void onMessage(TestObject t) {
					System.out.println("Thread " + Thread.currentThread().getId() + ":topic_test" + id + ":" + JSON.toJSONString(t));
					latch.countDown();
				}

				@Override
				public IJedissonSerializer<TestObject> getSerializer() {
					// TODO Auto-generated method stub
					return new JedissonFastJsonSerializer<TestObject>(TestObject.class);
				}
				
			};
			
			pubsub.subscribe("topic_test" + i, listener);
	
		}
				
		for(int i = 0; i < 1000; i++){
			TestObject test = new TestObject();
			test.setName("publish_test" + i);
			pubsub.publish("topic_test" + i, test);	
		}
		
		latch.await();
	}
	
	@Test
	public void testMultiPublish() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(1000);
		
		IJedisson jedisson = Jedisson.getJedisson();
		final IJedissonPubSub pubsub = jedisson.getPubSub("my_pubsub", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0; i < 1; i++){
			final int id = i;
			IJedissonMessageListener<TestObject> listener = new IJedissonMessageListener<TestObject>(){

				@Override
				public void onMessage(TestObject t) {
					System.out.println("Thread " + Thread.currentThread().getName() + ":topic_test" + id + ":" + JSON.toJSONString(t));
					latch.countDown();
				}

				@Override
				public IJedissonSerializer<TestObject> getSerializer() {
					// TODO Auto-generated method stub
					return new JedissonFastJsonSerializer<TestObject>(TestObject.class);
				}
				
			};
			
			pubsub.subscribe("topic_test", listener);
	
		}
				
		for(int i = 0; i < 10; i++){
			Thread thread = new Thread(){

				@Override
				public void run() {
					for(int j = 0; j < 100; j++){
						TestObject test = new TestObject();
						test.setName(Thread.currentThread().getId() + ":publish_test" + j);
						pubsub.publish("topic_test", test);	
					}
						
				}
				
			};
			thread.start();
			
		}

		latch.await();
	}
	
	@Test
	public void testMultiSubscribeAndPublish() throws InterruptedException{
		final CountDownLatch latch = new CountDownLatch(1000);
		
		IJedisson jedisson = Jedisson.getJedisson();
		final IJedissonPubSub pubsub = jedisson.getPubSub("my_pubsub", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0; i < 10; i++){
			final int id = i;
			IJedissonMessageListener<TestObject> listener = new IJedissonMessageListener<TestObject>(){

				@Override
				public void onMessage(TestObject t) {
					System.out.println("Thread " + Thread.currentThread().getName() + ":topic_test" + id + ":" + JSON.toJSONString(t));
					latch.countDown();
				}

				@Override
				public IJedissonSerializer<TestObject> getSerializer() {
					// TODO Auto-generated method stub
					return new JedissonFastJsonSerializer<TestObject>(TestObject.class);
				}
				
			};
			
			pubsub.subscribe("topic_test" + i, listener);
	
		}
				
		for(int i = 0; i < 10; i++){
			final int id = i;
			Thread thread = new Thread(){

				@Override
				public void run() {
					for(int j = 0; j < 100; j++){
						TestObject test = new TestObject();
						test.setName(Thread.currentThread().getId() + ":publish_test" + j);
						pubsub.publish("topic_test" + id, test);	
					}
						
				}
				
			};
			thread.start();
			
		}
		
		latch.await();
	}
}
