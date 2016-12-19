package org.jedisson.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import junit.framework.Assert;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.collection.JedissonList;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;

import com.alibaba.fastjson.JSON;

public class JedissonListTest extends BaseTest{
	
	@Before
	public void testBegin(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList",new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));
			list.add(test);
		}
	}
	
	@After
	public void testEnd(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList",new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		list.clear();
	}
	
	@Test
	public void testJedissonListAdd(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList",new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		Assert.assertEquals(10, list.size());
	}
	
	@Test
	public void testJedissonListIerator(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
	
		System.out.println("----------test foreach----------");
		for(TestObject v : list){
			System.out.println(JSON.toJSONString(v));
		}
		
		System.out.println("----------test iterator----------");
		Iterator<TestObject> iter = list.iterator();
		while(iter.hasNext()){
			TestObject v = iter.next();
			System.out.println(JSON.toJSON(v));
		}
		System.out.println("----------test listIterator----------");
		ListIterator<TestObject> lIter = list.listIterator();
		while(lIter.hasNext()){
			TestObject v = lIter.next();
			System.out.println(JSON.toJSONString(v));
		}
	}
	
	@Test
	public void testJedissonSize(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		Assert.assertEquals(10, list.size());
	}
	
	@Test
	public void tetsJedissonClear(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		list.clear();
		
		Assert.assertEquals(0, list.size());
	}
	
	@Test
	public void testJedissonIsEmpty(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		Assert.assertEquals(false, list.isEmpty());
	}
	
	@Test
	public void testJedissonContains(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		TestObject test = new TestObject();
		test.setName("test" + 2);
		test.setAge(2);
		test.getFriends().add("friends" + 2);
		test.getChilden().put("child" + 2, new TestObject("child" + 2,2));
		Assert.assertEquals(true, list.contains(test));
	}
	
	@Test
	public void testJedissonAddFirst(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		TestObject test = new TestObject();
		test.setName("first" + 0);
		test.setAge(0);
		test.getFriends().add("friends" + 0);
		test.getChilden().put("child" + 0, new TestObject("child" + 0,0));
		list.add(0, test);
		
		Assert.assertEquals(11, list.size());
		Assert.assertEquals("first0", list.get(0).getName());
	}
	
	@Test
	public void testJedissonAddLast(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		TestObject test = new TestObject();
		test.setName("last" + 0);
		test.setAge(0);
		test.getFriends().add("friends" + 0);
		test.getChilden().put("child" + 0, new TestObject("child" + 0,0));
		list.add(list.size(), test);
		Assert.assertEquals(11, list.size());
		Assert.assertEquals("last0", list.get(list.size() - 1).getName());
	}
	
	@Test
	public void testJedissonAddMiddle(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		TestObject test = new TestObject();
		test.setName("middle" + 0);
		test.setAge(0);
		test.getFriends().add("friends" + 0);
		test.getChilden().put("child" + 0, new TestObject("child" + 0,0));
		list.add(5, test);
		Assert.assertEquals(11, list.size());
		Assert.assertEquals("middle0", list.get(5).getName());
	}
	
	@Test
	public void testJedissonAddAll(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		List<TestObject> tests = new ArrayList<>();
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("addAll" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));	
			tests.add(test);
		}
		list.addAll(tests);
		Assert.assertEquals(20, list.size());
		Assert.assertEquals("addAll9", list.get(list.size() - 1).getName());
	}
	
	@Test
	public void testJedissonAddAllFirst(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		List<TestObject> tests = new ArrayList<>();
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("addAll" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));	
			tests.add(test);
		}
		list.addAll(0, tests);
		
		Assert.assertEquals(20, list.size());
		Assert.assertEquals("addAll0", list.get(0).getName());
	}
	
	@Test
	public void testJedissonAddAllLast(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		List<TestObject> tests = new ArrayList<>();
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("addAll" + i);
			test.setAge(i);
			test.getFriends().add("friends" +i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));	
			tests.add(test);
		}
		list.addAll(list.size(), tests);
		
		Assert.assertEquals(20, list.size());
		Assert.assertEquals("addAll9", list.get(list.size() - 1).getName());
	}
	
	@Test
	public void testJedissonAddAllMiddle(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		List<TestObject> tests = new ArrayList<>();
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("addAll" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));	
			tests.add(test);
		}
		list.addAll(7, tests);
		Assert.assertEquals(20, list.size());
		Assert.assertEquals("addAll9", list.get(16).getName());
	}
	
	@Test
	public void testJedissonIndexOf(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));	
			int index = list.indexOf(test);
			Assert.assertEquals(i, index);
		}
	}
	
	@Test
	public void testJedissonGet(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0 ; i < 10; i++){
			TestObject test = list.get(i);
			Assert.assertEquals("test" + i, test.getName());
		}
	}
	
	@Test
	public void testJedissonLastIndexOf(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		for(int i = 0 ; i < 10; i++){
			TestObject test = new TestObject();
			test.setName("test" + i);
			test.setAge(i);
			test.getFriends().add("friends" + i);
			test.getChilden().put("child" + i, new TestObject("child" + i,i));	
			int index = list.lastIndexOf(test);
			Assert.assertEquals(i, index);
		}
	}
	
	@Test
	public void testJedissonListIterator(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		int i = 1;
		ListIterator<TestObject> iter = list.listIterator(i);
		while(iter.hasNext()){
			TestObject test = iter.next();
			Assert.assertEquals("test" + i++, test.getName());
		}
	}
	
	@Test
	public void testJedissonRemove(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		TestObject test0 = list.remove(0);
		Assert.assertEquals("test0", test0.getName());
		Assert.assertEquals(9, list.size());
		Assert.assertEquals("test1", list.get(0).getName());
		
		TestObject test1 = list.remove(list.size() - 1);
		Assert.assertEquals("test9", test1.getName());
		Assert.assertEquals(8, list.size());
		Assert.assertEquals("test8", list.get(list.size() - 1).getName());
		
		TestObject test2 = list.remove(5);
		Assert.assertEquals(7, list.size());
		Assert.assertEquals("test6", test2.getName());
		
		TestObject test3 = list.get(3);
		Assert.assertEquals(true,list.remove(test3));
		Assert.assertEquals(6, list.size());
	}
	
	@Test
	public void testJedissonRemoveAll(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		List<TestObject> subList = new ArrayList<>();
		for(int i = 0; i < 5; i++){
			subList.add(list.get(i));
		}
		Assert.assertEquals(true, list.removeAll(subList));
		Assert.assertEquals(5, list.size());
	}
	
	@Test
	public void testJedissonRetainAll(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		List<TestObject> subList = new ArrayList<>();
		for(int i = 0; i < 5; i++){
			subList.add(list.get(i));
		}
		
		Assert.assertEquals(true, list.retainAll(subList));
		Assert.assertEquals(5, list.size());
		Assert.assertEquals("test4", list.get(list.size() - 1).getName());
	}
	
	@Test
	public void testJedissonSet(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		TestObject test = new TestObject();
		test.setName("testSet1");
		test.setAge(100);
		TestObject oldTest = list.set(0, test);
		
		Assert.assertEquals(10, list.size());
		Assert.assertEquals("test0", oldTest.getName());
		Assert.assertEquals("testSet1", list.get(0).getName());
		
		test.setName("testSet2");
		oldTest = list.set(4, test);
		Assert.assertEquals(10, list.size());
		Assert.assertEquals("test4", oldTest.getName());
		Assert.assertEquals("testSet2", list.get(4).getName());
		
		test.setName("testSet3");
		oldTest = list.set(list.size() - 1, test);
		Assert.assertEquals(10, list.size());
		Assert.assertEquals("test9", oldTest.getName());
		Assert.assertEquals("testSet3", list.get(list.size() - 1).getName());
	}
	
	@Test
	public void testJedissonToArray(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		
		Object[] objects = list.toArray();
		Assert.assertEquals(10, objects.length);
		
		TestObject[] tests = list.toArray(new TestObject[0]);
		Assert.assertEquals(10, tests.length);
		
		tests = list.toArray(new TestObject[20]);
		Assert.assertEquals("test9",tests[9].getName());
	}
	
	@Test
	public void testJedissonMultiThread(){
		IJedisson jedisson = Jedisson.getJedisson(redisTemplate);
		final JedissonList<TestObject> list = jedisson.getList("myList", new JedissonFastJsonSerializer<TestObject>(TestObject.class));
		final int count = 1000;
		Thread thread1 = new Thread(){

			@Override
			public void run() {
				for(int i = 0; i < count; i++){
					TestObject test = new TestObject();
					test.setName("thread" + i);
					test.setAge(i);
					list.add(test);
//					System.out.println(Thread.currentThread().getId() + ": " + "add " + test.getName());
				}
			}
			
		};
		
		thread1.start();
		Thread thread2 = new Thread(){

			@Override
			public void run() {
				while(!list.isEmpty()){
					TestObject test = list.get(0);
					list.remove(test);
//					System.out.println(Thread.currentThread().getId() + " remove " + test.getName());
				}
			}
		};
		thread2.start();
		
		try {
			thread1.join();
			thread2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
