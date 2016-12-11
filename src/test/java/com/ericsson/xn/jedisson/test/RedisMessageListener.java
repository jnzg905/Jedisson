package com.ericsson.xn.jedisson.test;

import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import com.alibaba.fastjson.JSON;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.serializer.JedissonFastJsonSerializer;

public class RedisMessageListener implements MessageListener{

	private IJedissonSerializer<TestObject> serializer = new JedissonFastJsonSerializer<TestObject>(TestObject.class);
	@Override
	public void onMessage(Message message, byte[] pattern) {
		TestObject test = serializer.deserialize(new String(message.getBody()));
		System.out.println(Thread.currentThread().getId() + ":" + JSON.toJSONString(test));
		
		
	}

}
