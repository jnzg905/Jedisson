package com.ericsson.xn.jedisson.test;

import org.junit.BeforeClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;

public class BaseTest {
	protected static ApplicationContext context;
	
	protected static RedisTemplate redisTemplate;
	
	@BeforeClass
	public static void beforeTest(){
		context = new ClassPathXmlApplicationContext("redis-config-test.xml");
		redisTemplate = (RedisTemplate) context.getBean("redisTemplate");
	}
}
