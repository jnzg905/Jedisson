package org.jedisson.api;

import org.jedisson.cache.JedissonCacheConfiguration;

public interface IJedissonCacheManager {
	
	public <K,V> IJedissonAsyncCache<K,V> getAsyncCache(final String name);
	
	public <K,V> IJedissonAsyncCache<K,V> getAsyncCache(final String name, final JedissonCacheConfiguration<K,V> configuration);
	
	public void removeCache(final String name);
}
