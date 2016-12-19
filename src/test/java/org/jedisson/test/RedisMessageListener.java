package org.jedisson.test;

import org.jedisson.api.IJedissonSerializer;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import com.alibaba.fastjson.JSON;

public class RedisMessageListener implements MessageListener{

	private IJedissonSerializer<TestObject> serializer = new JedissonFastJsonSerializer<TestObject>(TestObject.class);
	@Override
	public void onMessage(Message message, byte[] pattern) {
		TestObject test = serializer.deserialize(message.getBody());
		System.out.println(Thread.currentThread().getId() + ":" + JSON.toJSONString(test));
		
		
	}

}
