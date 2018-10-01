package org.jedisson.api;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.cache.Cache;

import org.jedisson.cache.JedissonCacheConfiguration;

public interface IJedissonAsyncCache<K,V> extends IJedissonAsyncSupport{
	public CompletableFuture<V> get(K key);
	
	public CompletableFuture<? extends Map<K,V>> getAll(Set<? extends K> keys);
	
	public CompletableFuture<Long> put(K key, V value);
	
	public CompletableFuture<V> getAndPut(K key, V value);
	
	public CompletableFuture<Boolean> containsKey(K key);

	public CompletableFuture<Boolean> putIfAbsent(K key, V value);
	
	public CompletableFuture<Void> putAll(java.util.Map<? extends K, ? extends V> map);
	
	public CompletableFuture<Long> remove(K key);
	
	public CompletableFuture<Long> removeAll(Set<? extends K> keys);
	
	public CompletableFuture<Long> removeAll();
	
	public CompletableFuture<V> getAndRemove(K key);
	
	public CompletableFuture<Long> clear();
	
	public Iterator<Cache.Entry<K, V>> iterator();
	
	public CompletableFuture<Long> size();
	
	public JedissonCacheConfiguration<K,V> getConfiguration();
}
