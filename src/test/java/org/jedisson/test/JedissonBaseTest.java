package org.jedisson.test;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonBlockingQueue;
import org.jedisson.common.BeanLocator;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.context.junit4.SpringRunner;

public class JedissonBaseTest {

	@Autowired
	private RedisConnectionFactory redisConnectionFactory;
	
	protected static IJedisson jedisson;
	
	public void begin() throws InterruptedException{
		if(jedisson == null){
			jedisson = Jedisson.builder().withFlushSize(6000)
					.withFlushFreq(100)
					.withRedisConnectionFactory(redisConnectionFactory).builder();	
		}
	}
}
