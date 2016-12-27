package org.jedisson.test;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.test.context.junit4.SpringRunner;

import com.alibaba.fastjson.JSON;

@RunWith(SpringRunner.class)
@SpringBootTest(classes=TestJedissonJsr107Cache.class)
@SpringBootApplication(scanBasePackages="org.jedisson")
public class TestJedissonJsr107Cache{

	@Before
	public void testBegin(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		MutableConfiguration<String, TestObject> configuration =
		        new MutableConfiguration<String, TestObject>()  
		            .setTypes(String.class, TestObject.class)   
		            .setStoreByValue(false)   
		            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		if(cache == null){
			cache = cacheManager.createCache("myCache", configuration);	
		}
				
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friend" + i);
			cache.put(test.getName(), test);
		}
	}
	
	@After
	public void testEnd(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		cache.clear();
	}
	
	@Test
	public void testGetCacheManager1(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Assert.assertEquals(provider.getDefaultURI(),cacheManager.getURI());
		Assert.assertEquals(this.getClass().getClassLoader(), cacheManager.getClassLoader());
	}
	
	@Test
	public void testGetCacheManager2(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager(URI.create("jedisson://testGetCacheManager2"),this.getClass().getClassLoader());
		
		Assert.assertEquals("jedisson://testGetCacheManager2", cacheManager.getURI().toString());
		Assert.assertEquals(this.getClass().getClassLoader(), cacheManager.getClassLoader());
	}
	
	@Test
	public void testGetCacheManager3(){
		Properties properties = new Properties();
		properties.put("test", "test");
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager(URI.create("jedisson://testGetCacheManager3"),
				this.getClass().getClassLoader(),properties);
		
		Assert.assertEquals("jedisson://testGetCacheManager3", cacheManager.getURI().toString());
		Assert.assertEquals(this.getClass().getClassLoader(), cacheManager.getClassLoader());
		Assert.assertEquals("test", cacheManager.getProperties().get("test"));
	}
	
	@Test
	public void testCreateCacheSuccess(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		MutableConfiguration<String, TestObject> configuration =
		        new MutableConfiguration<String, TestObject>()  
		            .setTypes(String.class, TestObject.class)   
		            .setStoreByValue(false)   
		            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE)); 
		Cache<String,TestObject> cache = cacheManager.createCache("testCache", configuration);
		TestObject test = new TestObject();
		test.setName("test");
		cache.put(test.getName(),test);
		Assert.assertEquals(true, cache != null);
		Assert.assertEquals("test", cache.get("test").getName());
		
		cacheManager.destroyCache("testCache");
	}
	
	@Test
	public void testCreateCacheFailed(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		MutableConfiguration<String, TestObject> configuration =
		        new MutableConfiguration<String, TestObject>()  
		            .setTypes(String.class, TestObject.class)   
		            .setStoreByValue(false)   
		            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE)); 
		Cache<String,TestObject> cache = cacheManager.createCache("testCache", configuration);
		boolean created = true;
		try{
			cache = cacheManager.createCache("testCache", configuration);
		}catch(Exception e){
			created = false;
		}
		
		Assert.assertEquals(false, created);
		
		cacheManager.destroyCache("testCache");
		
	}
	
	@Test
	public void testGetCache(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		for(int i = 0; i < 10; i++){
			TestObject test = cache.get("test" + i);
			Assert.assertEquals("test" + i, test.getName());
		}
		
	}
	
	@Test
	public void testCacheClose(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		cache.close();
		
		Assert.assertEquals(true, cache.isClosed());
		
	}
	
	@Test
	public void testCacheClear(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		cache.clear();
		
		Assert.assertEquals(false, cache.containsKey("test" + 5));
	}
	
	@Test
	public void testCacheContainKey(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		for(int i = 0; i < 10; i++){
			Assert.assertEquals(true, cache.containsKey("test" + i));
		}
	}
	
	@Test
	public void testCacheGetAll(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		Set<String> keys = new HashSet<>();
		for(int i = 0; i < 10; i++){
			keys.add("test" + i);
		}
		Map<String,TestObject> values = cache.getAll(keys);
		
		Assert.assertEquals(10, values.size());
		
	}
	
	@Test
	public void testCacheGetAndPut(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject test = new TestObject();
		test.setAge(100);
		test.getFriends().add("firend100");
		TestObject v = cache.getAndPut("test5", test);
		Assert.assertEquals("test5", v.getName());
		Assert.assertEquals(100, cache.get("test5").getAge());
	}
	
	@Test
	public void testCacheGetAndRemove(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject oldTest = cache.getAndRemove("test1");
		Assert.assertEquals(false, cache.containsKey("test1"));
		Assert.assertEquals("test1", oldTest.getName());
		
		oldTest = cache.getAndRemove("test10");
		Assert.assertEquals(null, oldTest);
	}
	
	@Test
	public void testCacheGetAndReplace(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject newTest = new TestObject();
		newTest.setName("newTest");
		newTest.setAge(1000);
		
		TestObject oldTest = cache.getAndReplace("test8", newTest);
		Assert.assertEquals(true,cache.containsKey("test8"));
		Assert.assertEquals("newTest", cache.get("test8").getName());
	}
	
	@Test
	public void testCachePut(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		for(int i = 10; i < 20; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(100 + i);
			cache.put(test.getName(), test);
			
			Assert.assertEquals("test" + i, cache.get("test" + i).getName());
		}
		
	}
	
	@Test
	public void testCachePutAll(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		Map<String,TestObject> map = new HashMap<>();
		
		for(int i = 10; i < 20; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(100 + i);
			map.put(test.getName(), test);
		}
		cache.putAll(map);
		
		Assert.assertEquals(true,cache.containsKey("test19"));
	}
	
	@Test
	public void testCachePutIfAbsent(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject test = new TestObject();
		test.setName("test10");
		test.setAge(10);
		Assert.assertEquals(true,cache.putIfAbsent(test.getName(), test));
		Assert.assertEquals("test10", cache.get("test10").getName());
		
		test.setName("test1");
		test.setAge(100000);
		Assert.assertEquals(false, cache.putIfAbsent(test.getName(), test));
		Assert.assertEquals(1, cache.get("test1").getAge());
	}
	
	@Test
	public void testCacheRemove(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		for(int i = 0; i < 10; i++){
			cache.remove("test" + i);
			Assert.assertEquals(null, cache.get("test" + i));
		}
		
	}
	
	@Test
	public void testCacheRemoveOldValue(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject test = cache.get("test1");
		
		Assert.assertEquals(true,cache.remove("test1", test));
		Assert.assertEquals(null, cache.get("test1"));
		
		test = new TestObject();
		test.setName("test2");
		Assert.assertEquals(false,cache.remove("test2", test));
		Assert.assertEquals("test2", cache.get("test2").getName());
	}
	
	@Test
	public void testCacheRemoveAll(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		cache.removeAll();
		
		Assert.assertEquals(false, cache.containsKey("test1"));
	}
	
	@Test
	public void testCacheRemoveAllBySet(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		Set<String> keys = new HashSet<>();
		
		for(int i = 0; i < 5; i++){
			keys.add("test" + i);
		}
		cache.removeAll(keys);
		
		Assert.assertEquals(false, cache.containsKey("test4"));
		Assert.assertEquals(true, cache.containsKey("test5"));
		
	}
	
	@Test
	public void testCacheReplace(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject test = new TestObject();
		test.setName("replace");
		test.setAge(99999);
		Assert.assertEquals(true,cache.replace("test3", test));
		Assert.assertEquals(99999, cache.get("test3").getAge());
	}
	
	@Test
	public void testCacheReplaceByOldValue(){
		CachingProvider provider = Caching.getCachingProvider();
		CacheManager cacheManager = provider.getCacheManager();
		Cache<String,TestObject> cache = cacheManager.getCache("myCache");
		
		TestObject oldValue = cache.get("test9");
		TestObject newValue = new TestObject();
		newValue.setName("newName");
		newValue.setAge(99);;
		Assert.assertEquals(true, cache.replace(oldValue.getName(), oldValue, newValue));
		Assert.assertEquals(true, cache.containsKey("test9"));
		Assert.assertEquals("newName", cache.get("test9").getName());
		
		oldValue = new TestObject();
		oldValue.setName("not_exists");
		oldValue.setAge(9);
		Assert.assertEquals(false, cache.replace(oldValue.getName(), oldValue, newValue));
	}
	
	@Test
	public void testFastJsonExpiredFactory(){
		Factory factory = CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE);
		
		JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer();
		byte[] bytes = serializer.serialize(factory);
		
		String str = new String(bytes);
		
		String obj = JSON.toJSONString(factory);
	}
	

}

