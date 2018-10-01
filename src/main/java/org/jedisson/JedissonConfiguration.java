package org.jedisson;

import org.jedisson.api.IJedissonRedisExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;

public class JedissonConfiguration {

	private String executor = "org.jedisson.RedisTemplateExecutor";
	
	private RedisConnectionFactory redisConnectionFactory;
	
	private String keySerializerType = "org.jedisson.serializer.JedissonStringSerializer";
	
	private String valueSerializerType = "org.jedisson.serializer.JedissonFastJsonSerializer";
	
	private String cacheManagerType = "org.jedisson.cache.JedissonCacheManager";

	private int flushThreadNum = 10;

	private int flushSize = 1000;
	
	private int flushFreq = 1000;
	
	public String getExecutor() {
		return executor;
	}

	public void setExecutor(String executor) {
		this.executor = executor;
	}

	public String getKeySerializerType() {
		return keySerializerType;
	}

	public void setKeySerializerType(String keySerializerType) {
		this.keySerializerType = keySerializerType;
	}

	public String getValueSerializerType() {
		return valueSerializerType;
	}

	public void setValueSerializerType(String valueSerializerType) {
		this.valueSerializerType = valueSerializerType;
	}

	public String getCacheManagerType() {
		return cacheManagerType;
	}

	public void setCacheManagerType(String cacheManagerType) {
		this.cacheManagerType = cacheManagerType;
	}

	public int getFlushThreadNum() {
		return flushThreadNum;
	}

	public void setFlushThreadNum(int flushThreadNum) {
		this.flushThreadNum = flushThreadNum;
	}

	public int getFlushSize() {
		return flushSize;
	}

	public void setFlushSize(int flushSize) {
		this.flushSize = flushSize;
	}

	public int getFlushFreq() {
		return flushFreq;
	}

	public void setFlushFreq(int flushFreq) {
		this.flushFreq = flushFreq;
	}

	public RedisConnectionFactory getRedisConnectionFactory() {
		return redisConnectionFactory;
	}

	public void setRedisConnectionFactory(RedisConnectionFactory redisConnectionFactory) {
		this.redisConnectionFactory = redisConnectionFactory;
	}
	
	
}
