package org.jedisson.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;

import org.jedisson.cache.JedissonCacheConfiguration;

public interface IJedissonCache<K,V>{
	
	public V get(K key);
	
	public Map<K,V> getAll(Set<? extends K> keys);
	
	public void put(K key, V value);
	
	public V getAndPut(K key, V value);
	
	public boolean containsKey(K key);

	public boolean putIfAbsent(K key, V value);
	
	public void putAll(java.util.Map<? extends K, ? extends V> map);
	
	public boolean remove(K key);
	
	public void removeAll(Set<? extends K> keys);
	
	public void removeAll();
	
	public V getAndRemove(K key);
	
	public void clear();
	
	Iterator<Cache.Entry<K, V>> iterator();
	
	public long size();
	
	public JedissonCacheConfiguration<K,V> getConfiguration();
}

