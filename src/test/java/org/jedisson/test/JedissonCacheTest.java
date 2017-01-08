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








import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonFuture;
import org.jedisson.cache.JedissonCacheConfiguration;
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
@SpringBootTest(classes=JedissonCacheTest.class)
@SpringBootApplication(scanBasePackages="org.jedisson")
public class JedissonCacheTest{
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
	public void testMultiThreadGetAndPut() throws InterruptedException{
		final IJedisson jedisson = Jedisson.getJedisson();
		
		final CountDownLatch latch = new CountDownLatch(100);

		ExecutorService executor = Executors.newFixedThreadPool(10);
		
		for(int i = 0; i < 100; i++){
			final int c = i;
			if(i % 2 == 0){
				executor.submit(new Runnable(){

					@Override
					public void run() {
						try{
							IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
							Iterator<Cache.Entry<String,TestObject>> iter = cache.iterator();
							while(iter.hasNext()){
								Cache.Entry<String,TestObject> entry = iter.next();
								System.out.println(JSON.toJSONString(entry));
							}	
						}catch(Exception e){
							e.printStackTrace();
						}
						finally{
							latch.countDown();	
						}
					}
					
				});
			}else{
				executor.submit(new Runnable(){

					@Override
					public void run() {
						try{
							IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
							for(int j = 0; j < 100; j++){
								TestObject test = new TestObject();
								test.setName("MultiThreadPut:" + c + ":" + j);
								test.setAge(j);
								test.getFriends().add("friends" + j);
								test.getChilden().put("child" + j, new TestObject("child" + j,j));
								cache.put(test.getName(), test);
							}	
						}catch(Exception e){
							e.printStackTrace();
						}finally{
							latch.countDown();	
						}
					}
				});
			}
			
		}
		latch.await();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		Assert.assertEquals(5010, cache.size());
		
	}
	
	@Test
	public void testJedissonAsyncCache() throws InterruptedException{
		IJedisson jedisson = Jedisson.getJedisson();
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		IJedissonCache<String,TestObject> asyncCache = cache.withAsync();
		
		long startTime = System.currentTimeMillis();
		List<IJedissonFuture<TestObject>> futures = new ArrayList<>();
		for(int i = 0; i < 1000000; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			asyncCache.put(test.getName(), test);
		}
		asyncCache.future().get();
		System.out.println("async put:" + (System.currentTimeMillis() - startTime));
		startTime = System.currentTimeMillis();
		for(int i = 0; i < 1000000; i++){
			asyncCache.get("test" + i);
//			futures.add(asyncCache.future());
		}
		
		TestObject test = (TestObject) asyncCache.future().get();
//		List<TestObject> results = new ArrayList<>();
//		for(IJedissonFuture<TestObject> future : futures){
//			results.add(future.get());
//		}
		System.out.println("async get:" + (System.currentTimeMillis() - startTime));
	}
}
