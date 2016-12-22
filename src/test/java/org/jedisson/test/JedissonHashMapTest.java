package org.jedisson.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.collection.JedissonList;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonStringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;

import com.alibaba.fastjson.JSON;

public class JedissonHashMapTest extends BaseTest{

	@Before
	public void testBegin(){
		IJedisson jedisson = Jedisson.getJedisson();
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
	
	@After
	public void testEnd(){
		IJedisson jedisson = Jedisson.getJedisson();
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		map.clear();
	}
	
	@Test
	public void testJedissonHashMapPut(){
		IJedisson jedisson = Jedisson.getJedisson();
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
		IJedisson jedisson = Jedisson.getJedisson();
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
		IJedisson jedisson = Jedisson.getJedisson();
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		for(int i = 0; i < 10; i++){
			TestObject test = map.get("test" + i);
			System.out.println(test.getName());
		}
	}
	
	@Test
	public void testJedissonMapKeyIterator(){
		IJedisson jedisson = Jedisson.getJedisson();
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
		IJedisson jedisson = Jedisson.getJedisson();
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
		IJedisson jedisson = Jedisson.getJedisson();
		JedissonHashMap<String,TestObject> map = jedisson.getMap("myMap",
				String.class,
				TestObject.class);
		
		for(TestObject test : map.values()){
			System.out.println(JSON.toJSONString(test));
		}
	}
	
	@Test
	public void testJedissonMapKeyIteratorRemove(){
		IJedisson jedisson = Jedisson.getJedisson();
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
		IJedisson jedisson = Jedisson.getJedisson();
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
		IJedisson jedisson = Jedisson.getJedisson();
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
}
