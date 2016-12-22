package org.jedisson;

import org.jedisson.api.IJedissonCacheManager;
import org.jedisson.api.IJedissonConfiguration;
import org.jedisson.api.IJedissonRedisExecutor;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonStringSerializer;

public class JedissonConfiguration implements IJedissonConfiguration{

	private IJedissonRedisExecutor executor;
	
	private String keySerializerType = "org.jedisson.serializer.JedissonStringSerializer";
	
	private String valueSerializerType = "org.jedisson.serializer.JedissonFastJsonSerializer";
	
	private String cacheManagerType = "org.jedisson.cache.JedissonCacheManager";
	
	@Override
	public IJedissonRedisExecutor getExecutor() {
		// TODO Auto-generated method stub
		return executor;
	}

	@Override
	public String getKeySerializerType() {
		// TODO Auto-generated method stub
		return keySerializerType;
	}

	@Override
	public String getValueSerializerType() {
		// TODO Auto-generated method stub
		return valueSerializerType;
	}

	public void setExecutor(IJedissonRedisExecutor executor) {
		this.executor = executor;
	}

	public void setKeySerializerType(String keySerializerType) {
		this.keySerializerType = keySerializerType;
	}

	public void setValueSerializerType(String valueSerializerType) {
		this.valueSerializerType = valueSerializerType;
	}

	public String getCacheManagerType() {
		return cacheManagerType;
	}

	public void setCacheManagerType(String cacheManager) {
		this.cacheManagerType = cacheManager;
	}
	
}
