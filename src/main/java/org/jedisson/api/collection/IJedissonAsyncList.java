package org.jedisson.api.collection;

import java.util.Collection;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface IJedissonAsyncList<V> extends IJedissonAsyncCollection<V>{
	
    CompletableFuture<Boolean> addAll(int index, Collection<? extends V> c);
    
    CompletableFuture<V> get(int index);

    CompletableFuture<V> set(int index, V element);

    CompletableFuture<Boolean> add(int index, V element);
    
    CompletableFuture<Long> indexOf(Object o);

    CompletableFuture<Long> lastIndexOf(Object o);

    CompletableFuture<List<V>> subList(int fromIndex, int toIndex);
    
    CompletableFuture<V> remove(int index);

}
