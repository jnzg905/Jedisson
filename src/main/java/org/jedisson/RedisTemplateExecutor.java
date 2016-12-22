package org.jedisson;

import java.util.List;

import org.jedisson.api.IJedissonRedisExecutor;
import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

public class RedisTemplateExecutor implements IJedissonRedisExecutor{

	private RedisTemplate redisTemplate;
	
	public RedisTemplate getRedisTemplate() {
		return redisTemplate;
	}

	public void setRedisTemplate(RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	@Override
	public <T> T execute(RedisCallback<T> callback) {
		return (T) redisTemplate.execute(callback,true);
	}

	@Override
	public <T> T execute(RedisScript<T> script,
			IJedissonSerializer<?> resultSerializer, List keys, Object... args) {
		return (T) redisTemplate.execute(
				script,
				(IJedissonSerializer)null,
				resultSerializer,
				keys,
				args);
	}

	@Override
	public RedisConnectionFactory getConnectionFactory() {
		return redisTemplate.getConnectionFactory();
	}

}
