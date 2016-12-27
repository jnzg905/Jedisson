package org.jedisson.common;

import java.util.List;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.serializer.RedisSerializer;

public class JedissonScriptExecutor<T> extends DefaultScriptExecutor<T>{

	public JedissonScriptExecutor(RedisTemplate template) {
		super(template);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected byte[][] keysAndArgs(RedisSerializer argsSerializer,
			List<T> keys, Object[] args) {
		return super.keysAndArgs(argsSerializer, keys, args);
	}

}
