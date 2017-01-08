package org.jedisson.api;

import java.util.List;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

public interface IJedissonRedisExecutor {

	public List<Object> executePipeline(RedisCallback<?> callback, IJedissonSerializer<?> resultSerializer);
	
	public <T> T execute(RedisCallback<T> callback);	

	public <T> T execute(RedisScript<T> script, IJedissonSerializer<?> resultSerializer, List keys, Object... args);
	
	public RedisConnectionFactory getConnectionFactory();
}
