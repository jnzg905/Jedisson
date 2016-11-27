package com.ericsson.xn.jedisson.api;

import com.ericsson.xn.jedisson.collection.JedissonList;
import com.ericsson.xn.jedisson.map.JedissonHashMap;

public interface IJedisson {

	public <V> JedissonList<V> getList(final String name, IJedissonSerializer serializer);
	
	public <K,V> JedissonHashMap<K,V> getMap(final String name, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer);
}
