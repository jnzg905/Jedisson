package org.jedisson.api;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public interface IJedissonSerializer<T> extends RedisSerializer<T>{

	public T deserialize(byte[] bytes, Class<T> clss) throws SerializationException;
}
