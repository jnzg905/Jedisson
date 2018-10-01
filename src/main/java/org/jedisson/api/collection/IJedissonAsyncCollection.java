package org.jedisson.api.collection;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jedisson.api.IJedissonAsyncSupport;

public interface IJedissonAsyncCollection<T> extends IJedissonAsyncSupport{
	
	CompletableFuture<Long> size();

    boolean isEmpty();

    boolean contains(Object o);

    CompletableFuture<Long> add(T e);

    CompletableFuture<Long> remove(Object o);

    CompletableFuture<Boolean> containsAll(Collection<?> c);

    CompletableFuture<Long> addAll(Collection<? extends T> c);

    CompletableFuture<Boolean> removeAll(Collection<?> c);

    CompletableFuture<Boolean> retainAll(Collection<?> c);

    CompletableFuture<Long> clear();
    
    CompletableFuture<List<T>> toArray();
    
}
