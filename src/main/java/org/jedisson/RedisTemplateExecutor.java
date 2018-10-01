package org.jedisson;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jedisson.api.IJedissonRedisExecutor;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonScriptExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

public class RedisTemplateExecutor implements IJedissonRedisExecutor{

	private RedisTemplate redisTemplate;
	
	public RedisTemplateExecutor(RedisConnectionFactory redisConnectionFactory){
		redisTemplate = new RedisTemplate<Object, Object>();
		redisTemplate.setConnectionFactory(redisConnectionFactory);
		redisTemplate.setEnableDefaultSerializer(false);
		redisTemplate.setScriptExecutor(new JedissonScriptExecutor(redisTemplate));
		redisTemplate.afterPropertiesSet();
	}
	
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
	public <T> T execute(RedisScript<?> script,
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

	@Override
	public List<Object> executePipeline(RedisCallback<?> callback, IJedissonSerializer<?> resultSerializer) {
		return redisTemplate.executePipelined(callback, resultSerializer);
	}

	@Override
	public Object deserializeMixedResults(Object rawValue, RedisSerializer valueSerializer) {
		if (rawValue == null) {
			return null;
		}
		if(rawValue instanceof byte[] && valueSerializer != null){
			return valueSerializer.deserialize((byte[]) rawValue);
		}else if(rawValue instanceof List){
			List rawList = (List)rawValue;
			return rawList.stream().map(v -> deserializeMixedResults(v,valueSerializer)).collect(Collectors.toList());
		}else if(rawValue instanceof Set && !((Set) rawValue).isEmpty()){
			return deserializeSet((Set) rawValue,valueSerializer);
		}else if(rawValue instanceof Map && !((Map) rawValue).isEmpty()){
			return SerializationUtils.deserialize((Map<byte[], byte[]>) rawValue, redisTemplate.getKeySerializer(),redisTemplate.getValueSerializer());
		}else{
			return rawValue;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Set<?> deserializeSet(Set rawSet, RedisSerializer valueSerializer) {
		if (rawSet.isEmpty()) {
			return rawSet;
		}
		Object setValue = rawSet.iterator().next();
		if (setValue instanceof byte[] && valueSerializer != null) {
			return (SerializationUtils.deserialize((Set) rawSet, valueSerializer));
		} else if (setValue instanceof Tuple) {
			return convertTupleValues(rawSet, valueSerializer);
		} else {
			return rawSet;
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Set<TypedTuple> convertTupleValues(Set<Tuple> rawValues, RedisSerializer valueSerializer) {
		Set<TypedTuple> set = new LinkedHashSet<TypedTuple>(rawValues.size());
		for (Tuple rawValue : rawValues) {
			Object value = rawValue.getValue();
			if (valueSerializer != null) {
				value = valueSerializer.deserialize(rawValue.getValue());
			}
			set.add(new DefaultTypedTuple(value, rawValue.getScore()));
		}
		return set;
	}
}
