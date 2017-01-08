package org.jedisson.api;

import java.util.Map;

public interface IJedissonMap<K,V> extends Map<K,V>, IJedissonAsyncSupport{

	public void fastPut(K key, V value);
}
