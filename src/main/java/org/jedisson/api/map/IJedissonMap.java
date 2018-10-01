package org.jedisson.api.map;

import java.util.Map;

import org.jedisson.api.IJedissonAsyncSupport;

public interface IJedissonMap<K,V> extends Map<K,V>{

	public void fastPut(K key, V value);
}
