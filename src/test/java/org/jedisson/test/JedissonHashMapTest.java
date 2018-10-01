package org.jedisson.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.api.map.IJedissonAsyncMap;
import org.jedisson.api.map.IJedissonMap;
import org.jedisson.map.JedissonHashMap;
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
@SpringBootTest(classes=JedissonHashMapTest.class)
@SpringBootApplication(scanBasePackages="org.jedisson")
public class JedissonHashMapTest extends JedissonBaseTest{

	@BeforeClass
	public void begin() throws InterruptedException{
		super.begin();
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",String.class,TestObject.class);
		
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			map.put(test.getName(), test);
		}
	}
	
	@AfterClass
	public void testEnd(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		map.clear();
	}
	
	@Test
	public void testJedissonHashMapPut(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		long startTime = System.currentTimeMillis();
		System.out.println("begin:" + startTime);
		int count = 1000;
		for(int i = 0; i < count; i++){
			TestObject test = new TestObject();
			test.setName("PutTest" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			map.put(test.getName(), test);
		}
		System.out.println("end:" + (System.currentTimeMillis() - startTime));
		Assert.assertEquals(count + 10, map.size());
		Assert.assertEquals("PutTest9", map.get("PutTest9").getName());
	}
	
	@Test
	public void testJedissonMapPutAll(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		long startTime = System.currentTimeMillis();
		System.out.println("begin:" + startTime);
		
		Map<String, TestObject> data = new HashMap<>();
		int count = 100000;
		for(int i = 0; i < count; i++){
			TestObject test = new TestObject();
			test.setName("PutTest" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			data.put(test.getName(), test);
		}
		
		map.putAll(data);
		System.out.println("end:" + (System.currentTimeMillis() - startTime));
		Assert.assertEquals(count + 10, map.size());
		Assert.assertEquals("PutTest9", map.get("PutTest9").getName());
	}
	@Test
	public void testJedissonHashMapGet(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		for(int i = 0; i < 10; i++){
			TestObject test = map.get("test" + i);
			Assert.assertEquals("test" + i, test.getName());
		}
	}
	
	@Test
	public void testJedissonMapKeyIterator(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		Iterator<String> iter = map.keySet().iterator();
		while(iter.hasNext()){
			String key = iter.next();
			TestObject test = map.get(key);
			System.out.println("key=" + key + ", value" + JSON.toJSONString(test));
		}
	}
	
	@Test
	public void testJedissonMapEntryIterator(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		Iterator<Entry<String,TestObject>> iter = map.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,TestObject> entry = iter.next();
			System.out.println("key=" + entry.getKey() + ", value" + JSON.toJSONString(entry.getValue()));
		}
	}
	
	@Test
	public void testJedissonMapValues(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		for(TestObject test : map.values()){
			System.out.println(JSON.toJSONString(test));
		}
	}
	
	@Test
	public void testJedissonMapKeyIteratorRemove(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		Iterator<String> iter = map.keySet().iterator();
		while(iter.hasNext()){
			String key = iter.next();
			TestObject test = map.get(key);
			iter.remove();
			System.out.println("key=" + key + ", value" + JSON.toJSONString(test));
		}
		
		Assert.assertEquals(0, map.size());
	}
	
	@Test
	public void testJedissonMapEntryIteratorRemove(){
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		Iterator<Entry<String,TestObject>> iter = map.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String,TestObject> entry = iter.next();
			iter.remove();
			System.out.println("key=" + entry.getKey() + ", value" + JSON.toJSONString(entry.getValue()));
		}
		
		Assert.assertEquals(0, map.size());
	}
	
	@Test
	public void testJedissonMapToMap(){
		JedissonHashMap<String,JedissonHashMap> map = jedisson.getMap("mapmap",
				String.class,
				JedissonHashMap.class);
		
		JedissonHashMap<String,TestObject> valMap = jedisson.getMap("valMap", 
				String.class,
				TestObject.class);
		
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			valMap.put("valMap_key" + i, test);	
		}
		map.put("mapmap_key1", valMap);
		
		map.clear();
		valMap.clear();
	}
	
	@Test
	public void testJedissonAsyncMap() throws InterruptedException, ExecutionException{
		IJedissonAsyncMap<String,TestObject> map = jedisson.getAsyncMap("asyncMap",String.class,TestObject.class);
		
		final int count = 1000000;
		{
			long startTime = System.currentTimeMillis();
			List<CompletableFuture> futures = new ArrayList<>();
			for(int i = 0; i < count; i++){
				TestObject test = new TestObject();
				test.setName("test" + i);
				test.setAge(i);
				test.getFriends().add("friends" + i);
				test.getChilden().put("child" + i, new TestObject("child" + i,i));
				futures.add(map.fastPut(test.getName(), test));
			}
			
			futures.stream().forEach(f -> f.join());
			
			Assert.assertEquals(count, map.size().join().intValue());
			System.out.println("async put:" + count * 1000.0f / (System.currentTimeMillis() - startTime));	
		}
		
		{
			List<CompletableFuture> futures = new ArrayList<>();
			long startTime = System.currentTimeMillis();
			for(int i = 0; i < count; i++){
				futures.add(map.get("test" + i));
			}
			
			futures.stream().forEach(f -> f.join());
			System.out.println("async get:" + count * 1000.0f / (System.currentTimeMillis() - startTime));
			map.clear();
		}
		
	
	}
}
