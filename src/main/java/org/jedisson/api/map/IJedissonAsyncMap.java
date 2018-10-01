package org.jedisson.api.map;


import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.jedisson.api.IJedissonAsyncSupport;

public interface IJedissonAsyncMap<K,V> extends IJedissonAsyncSupport{
    
	  CompletableFuture<Long> size();

	  CompletableFuture<Boolean> isEmpty();

	  CompletableFuture<Boolean> containsKey(K key);

	  CompletableFuture<Boolean> containsValue(V value);

	  CompletableFuture<V> get(K key);

	  CompletableFuture<V> put(K key, V value);

	  CompletableFuture<Long> fastPut(K key, V value);
	  
	  CompletableFuture<V> remove(K key);

	  CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m);

	  CompletableFuture<Long> clear();

//	  AsyncDistributedSet<K> keySet();
//
	  CompletableFuture<Collection<V>> values();

//	  AsyncDistributedSet<Map.Entry<K, V>> entrySet();


	  CompletableFuture<V> putIfAbsent(K key, V value);
	  
	  CompletableFuture<Long> fastRemove(final K... keys);
}
