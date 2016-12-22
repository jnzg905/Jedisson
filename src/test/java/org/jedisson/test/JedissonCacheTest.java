package org.jedisson.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Set;

import javax.cache.Cache;

import junit.framework.Assert;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonCache;
import org.jedisson.cache.JedissonCacheConfiguration;
import org.jedisson.collection.JedissonList;
import org.jedisson.lock.JedissonLock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;

public class JedissonCacheTest extends BaseTest{
	@Before
	public void testBegin(){
		IJedisson jedisson = Jedisson.getJedisson();
		JedissonCacheConfiguration<String,TestObject> configuration = 
				new JedissonCacheConfiguration<String,TestObject>()
				.setKeyType(String.class)
				.setValueType(TestObject.class);
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache", configuration);
		
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			cache.put(test.getName(), test);
		}
	}
	
	@After
	public void testEnd(){
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		cache.clear();
	}
	
	@Test
	public void testCacheGet(){
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		
		for(int i = 0; i < 10; i++){
			TestObject test = cache.get("test" + i);
			Assert.assertEquals("test" + i, test.getName());
		}
	}
	
	@Test
	public void testCacheGetAll(){
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		
		Set<String> keys = new HashSet<String>();
		for(int i = 0; i < 10; i++){
			keys.add("test" + i);
		}
		
		Map<String,TestObject> values = cache.getAll(keys);
		
		int i = 0;
		Iterator<Entry<String,TestObject>> iter = values.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,TestObject> entry = iter.next();
			System.out.println(JSON.toJSONString(entry));
		}
	}
	
	@Test
	public void testGetAndPut(){
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("getAndPut" + i);
			test.setAge(i + 100);
			test.getFriends().add("friends" + "getAndPut");
			TestObject oldTest = cache.getAndPut("test" + i, test);
			Assert.assertEquals("test" + i, oldTest.getName());
		}
		
		Iterator<Cache.Entry<String,TestObject>> iter = cache.iterator();
		while(iter.hasNext()){
			Cache.Entry<String,TestObject> entry = iter.next();
			System.out.println(JSON.toJSONString(entry));
		}
	}
	
	@Test
	public void testMultiThreadPut() throws InterruptedException{
		final IJedisson jedisson = Jedisson.getJedisson();
		
		final CountDownLatch latch = new CountDownLatch(100);

		ExecutorService executor = Executors.newFixedThreadPool(10);
		
		for(int i = 0; i < 100; i++){
			final int c = i;
			executor.submit(new Runnable(){

				@Override
				public void run() {
					IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
					for(int j = 0; j < 1000; j++){
						TestObject test = new TestObject();
						test.setName("MultiThreadPut:" + c + ":" + j);
						test.setAge(j);
						test.getFriends().add("friends" + j);
						test.getChilden().put("child" + j, new TestObject("child" + j,j));
						cache.put(test.getName(), test);
					}
					
					latch.countDown();
				}
			});
		}
		latch.await();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		Assert.assertEquals(100010, cache.size());
		
	}
}
