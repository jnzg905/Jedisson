package org.jedisson.api;

public interface IJedissonConfiguration {
	
	public IJedissonRedisExecutor getExecutor();
	
	public String getCacheManagerType();
	
	public String getKeySerializerType();
	
	public String getValueSerializerType();
	
}
