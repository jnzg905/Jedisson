package org.jedisson.api;

import org.jedisson.collection.JedissonList;
import org.jedisson.lock.JedissonLock;
import org.jedisson.map.JedissonHashMap;

public interface IJedisson {

	public <V> JedissonList<V> getList(final String name, IJedissonSerializer<V> serializer);
	
	public <K,V> JedissonHashMap<K,V> getMap(final String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer<V> valueSerializer);
	
	public JedissonLock getLock(final String name);
	
	public IJedissonPubSub getPubSub(final String name, IJedissonSerializer serializer);
}
