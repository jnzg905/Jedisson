package org.jedisson.api;

import org.jedisson.JedissonConfiguration;
import org.jedisson.api.collection.IJedissonAsyncList;
import org.jedisson.cache.JedissonCacheConfiguration;
import org.jedisson.lock.JedissonLock;
import org.jedisson.lock.JedissonReentrantLock;
import org.jedisson.map.JedissonAsyncHashMap;
import org.jedisson.map.JedissonHashMap;

public interface IJedisson {

	public JedissonConfiguration getConfiguration();
	
	public IJedissonRedisExecutor getExecutor();
	
	public <V> IJedissonList<V> getList(final String name, Class<V> clss);
	
	public <V> IJedissonList<V> getList(final String name, IJedissonSerializer serializer);
	
	public <V> IJedissonAsyncList<V> getAsyncList(final String name, Class<V> clss);
	
	public <V> IJedissonAsyncList<V> getAsyncList(final String name, IJedissonSerializer serializer);
		
	public <K,V> JedissonHashMap<K,V> getMap(final String name, Class<K> keyClss, Class<V> valueClss);
	
	public <K,V> JedissonHashMap<K,V> getMap(final String name, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer);
	
	public <K,V> JedissonAsyncHashMap<K,V> getAsyncMap(final String name, Class<K> keyClss, Class<V> valueClss);
	
	public <K,V> JedissonAsyncHashMap<K,V> getAsyncMap(final String name, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer);
	
	public JedissonLock getLock(final String name);
	
	public JedissonReentrantLock getReentrantLock(final String name);
	 
	public IJedissonPubSub getPubSub(final String name);
	
	public IJedissonPubSub getPubSub(final String name, IJedissonSerializer serializer);
	
	public IJedissonAsyncPubSub getAsyncPubSub(final String name);
	
	public IJedissonAsyncPubSub getAsyncPubSub(final String name, IJedissonSerializer serializer);
	
	public <K,V> IJedissonCache<K,V> getCache(final String name);
	
	public <K,V> IJedissonCache<K,V> getCache(final String name, JedissonCacheConfiguration<K,V> cacheConfiguration);
	
	public <K,V> IJedissonAsyncCache<K,V> getAsyncCache(final String name);
	
	public <K,V> IJedissonAsyncCache<K,V> getAsyncCache(final String name, JedissonCacheConfiguration<K,V> cacheConfiguration);
	
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(final String name, Class<V> clss);
	
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(final String name, IJedissonSerializer serializer);
	
	
}
