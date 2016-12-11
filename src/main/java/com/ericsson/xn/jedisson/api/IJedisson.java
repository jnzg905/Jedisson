package com.ericsson.xn.jedisson.api;

import com.ericsson.xn.jedisson.collection.JedissonList;
import com.ericsson.xn.jedisson.lock.JedissonLock;
import com.ericsson.xn.jedisson.map.JedissonHashMap;

public interface IJedisson {

	public <V> JedissonList<V> getList(final String name, IJedissonSerializer<V> serializer);
	
	public <K,V> JedissonHashMap<K,V> getMap(final String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer<V> valueSerializer);
	
	public JedissonLock getLock(final String name);
	
	public IJedissonPubSub getPubSub(final String name, IJedissonSerializer serializer);
}
