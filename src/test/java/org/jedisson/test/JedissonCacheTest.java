package org.jedisson.test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Set;

import javax.cache.Cache;

import org.jedisson.api.IJedissonAsyncCache;
import org.jedisson.api.IJedissonCache;
import org.jedisson.cache.JedissonCacheConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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
public class JedissonCacheTest extends JedissonBaseTest{
	
	@Before
	public void begin() throws InterruptedException{
		super.begin();
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
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		cache.clear();
	}
	
	@Test
	public void testCacheGet(){
		IJedissonCache<String,TestObject> cache = jedisson.getCache("myCache");
		
		for(int i = 0; i < 10; i++){
			TestObject test = cache.get("test" + i);
			Assert.assertEquals("test" + i, test.getName());
		}
	}
	
	@Test
	public void testCacheGetAll(){
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
	public void testJedissonAsyncCache() throws InterruptedException, ExecutionException{
		IJedissonAsyncCache<String,TestObject> cache = jedisson.getAsyncCache("myCache");
				
		long startTime = System.currentTimeMillis();
		List<CompletableFuture<Long>> futures = new ArrayList<>();
		for(int i = 0; i < 100000; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			futures.add(cache.put(test.getName(), test));
		}
		futures.stream().forEach(f -> f.join());
		System.out.println("async put:" + (System.currentTimeMillis() - startTime));
	}
	
	@Test
	public void testJedissonCachePerformance() throws InterruptedException, ExecutionException{
		JedissonCacheConfiguration<String,TestObject> configuration = 
				new JedissonCacheConfiguration<String,TestObject>()
				.setKeyType(String.class)
				.setValueType(TestObject.class);
		IJedissonCache<String,TestObject> cache = jedisson.getCache("testCache",configuration);
		
		int count = 300000;
		{
//			System.out.println("begin test single thread put:" + count);
//			long startTime = System.currentTimeMillis();
//			for(int i = 0; i < count; i++){
//				TestObject test = new TestObject();
//				test.setName("test" + i);
//				test.setAge(i);
//				test.getFriends().add("friends" + i);
//				test.getChilden().put("child" + i, new TestObject("child" + i,i));
//				cache.put(test.getName(), test);
//			}
//			System.out.println("single thread put time:" + count*1000.0f / (System.currentTimeMillis() - startTime));
//			cache.clear();	
		}
		
		{
			IJedissonAsyncCache<String,TestObject> asyncCache = jedisson.getAsyncCache("testCache", configuration);
			List<CompletableFuture> futures = new ArrayList<>();
			System.out.println("begin test single thread async put:" + count);
			long startTime = System.currentTimeMillis();
			for(int i = 0; i < count; i++){
				TestObject test = new TestObject();
				test.setName("test" + i);
				test.setAge(i);
				test.getFriends().add("friends" + i);
				test.getChilden().put("child" + i, new TestObject("child" + i,i));
				futures.add(asyncCache.put(test.getName(), test));
			}
			
			futures.stream().forEach(f -> f.join());
			System.out.println("single thread asyncPut time:" + count * 1000.0f / (System.currentTimeMillis() - startTime));
			cache.clear();
		}
		
		{
//			ExecutorService executor = Executors.newFixedThreadPool(10);
//			List<Future> futures = new ArrayList<>();
//			System.out.println("begin multi thread put:" + count);		
//			long startTime = System.currentTimeMillis();
//			for(int i = 0; i < 10; i++){
//				int t = i;
//				futures.add(executor.submit(new Callable<Boolean>(){
//
//					@Override
//					public Boolean call() throws Exception {
//						for(int j = t * count / 10; j < (t +1) * count / 10; j++){
//							TestObject test = new TestObject();
//							test.setName("test" + j);
//							test.setAge(j);
//							test.getFriends().add("friends" + j);
//							test.getChilden().put("child" + j, new TestObject("child" + j,j));
//							cache.put(test.getName(), test);
//						}
//						return true;
//					}
//					
//				}));
//			}
//			
//			futures.stream().forEach(f -> {
//				try {
//					f.get();
//				} catch (InterruptedException | ExecutionException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			});
//			System.out.println("multi thread put:" + count * 1000.0f / (System.currentTimeMillis() - startTime));
//			cache.clear();			
		}
		
		{
			ExecutorService executor = Executors.newFixedThreadPool(10);
			IJedissonAsyncCache<String,TestObject> asyncCache = jedisson.getAsyncCache("testCache", configuration);
			System.out.println("begin multi thread async put:" + count);
			List<Future> futures = new ArrayList<>();
			long startTime = System.currentTimeMillis();
			for(int i = 0; i < 10; i++){
				int t = i;
				futures.add(executor.submit(new Callable<List<CompletableFuture>>(){

					@Override
					public List<CompletableFuture> call() throws Exception {
						List<CompletableFuture> cfutures = new ArrayList<>();
						for(int j =  t * count / 10; j < (t + 1) * count / 10; j++){
							TestObject test = new TestObject();
							test.setName("test" + j);
							test.setAge(j);
							test.getFriends().add("friends" + j);
							test.getChilden().put("child" + j, new TestObject("child" + j,j));
							cfutures.add(asyncCache.put(test.getName(), test));
						}
						return cfutures;
					}
					
				}));
			}
					
			futures.stream().forEach(f -> {
				try{
					List<CompletableFuture> cfs = (List<CompletableFuture>) f.get();
					cfs.forEach(cf -> cf.join());	
				}catch(Exception e){
					e.printStackTrace();
				}
				
			});
			System.out.println("multi thread async put:" + count * 1000.0f / (System.currentTimeMillis() - startTime));
			cache.clear();
		}
		
		
	}
}
