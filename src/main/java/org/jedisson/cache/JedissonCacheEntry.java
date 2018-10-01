package org.jedisson.cache;

import javax.cache.Cache;

public class JedissonCacheEntry<K,V> implements Cache.Entry<K,V>{
	private K key;
	private V value;

	public JedissonCacheEntry(K key, V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}
}
