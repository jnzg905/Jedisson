package org.jedisson.autoconfiguration;

import org.jedisson.api.IJedissonRedisExecutor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;

@ConfigurationProperties(prefix = "jedisson")
public class JedissonConfiguration {

	private IJedissonRedisExecutor executor;
	
	private String keySerializerType = "org.jedisson.serializer.JedissonStringSerializer";
	
	private String valueSerializerType = "org.jedisson.serializer.JedissonFastJsonSerializer";
	
	private String cacheManagerType = "org.jedisson.cache.JedissonCacheManager";

	private Async async;
	
	public Async getAsync() {
		return async;
	}

	public void setAsync(Async async) {
		this.async = async;
	}

	

	public IJedissonRedisExecutor getExecutor() {
		return executor;
	}

	public void setExecutor(IJedissonRedisExecutor executor) {
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
	
	public static class Async{
		private int threadNum;

		private int flushSize;
		
		private int flushFreq;
		
		public int getThreadNum() {
			return threadNum;
		}

		public void setThreadNum(int threadNum) {
			this.threadNum = threadNum;
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
		
		
	}
	
}
