package org.jedisson.autoconfiguration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "jedisson")
public class JedissonProperties {

	private String keySerializerType = "org.jedisson.serializer.JedissonStringSerializer";
	
	private String valueSerializerType = "org.jedisson.serializer.JedissonFastJsonSerializer";
	
	private String cacheManagerType = "org.jedisson.cache.JedissonCacheManager";

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
	
	
}
