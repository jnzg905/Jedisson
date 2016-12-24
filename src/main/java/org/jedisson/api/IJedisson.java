package org.jedisson.api;

import org.jedisson.collection.JedissonList;
import org.jedisson.lock.JedissonLock;
import org.jedisson.lock.JedissonReentrantLock;
import org.jedisson.map.JedissonHashMap;

public interface IJedisson {

	public <V> IJedissonList<V> getList(final String name, Class<V> clss);
	
	public <V> IJedissonList<V> getList(final String name, Class<V> clss, IJedissonSerializer serializer);
	
	public <K,V> JedissonHashMap<K,V> getMap(final String name, Class<K> keyClss, Class<V> valueClss);
	
	public <K,V> JedissonHashMap<K,V> getMap(final String name, Class<K> keyClss, Class<V> valueClss, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer);
	
	public JedissonLock getLock(final String name);
	
	public JedissonReentrantLock getReentrantLock(final String name);
	 
	public IJedissonPubSub getPubSub(final String name);
	
	public IJedissonPubSub getPubSub(final String name, IJedissonSerializer serializer);
	
	public <K,V> IJedissonCache<K,V> getCache(final String name);
	
	public <K,V> IJedissonCache<K,V> getCache(final String name, IJedissonCacheConfiguration<K,V> cacheConfiguration);
	
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(final String name, Class<V> clss);
	
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(final String name, Class<V> clss, IJedissonSerializer serializer);
	
	
}
