package org.jedisson.api;

public interface IJedissonCacheManager {

	public <K,V> IJedissonCache<K,V> getCache(final String name);
	
	public <K,V> IJedissonCache<K,V> getCache(final String name, final IJedissonCacheConfiguration<K,V> configuration);
	
	public void removeCache(final String name);
}
