package org.jedisson.api;

import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

public interface IJedissonCacheConfiguration<K,V> {
		
	Class<K> getKeyType();
	
	Class<V> getValueType();
	
	boolean isReadThrough();
	
	boolean isWriteThrough();
	
	boolean isStatisticsEnabled();
	
	boolean isManagementEnabled();
	 
	Factory<CacheLoader<K, V>> getCacheLoaderFactory();
	
	Factory<CacheWriter<? super K, ? super V>> getCacheWriterFactory();
	
	Factory<ExpiryPolicy> getExpiryPolicyFactory();
}
