package com.ericsson.xn.jedisson.map;

import java.util.Map;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedissonObject;

public abstract class AbstractJedissonMap<K,V> extends JedissonObject implements Map<K,V>{

	private final IJedissonSerializer<K> keySerializer;
	
	private final IJedissonSerializer<V> valueSerializer;
	
	public AbstractJedissonMap(String name, IJedissonSerializer<K> keySerializer,
			IJedissonSerializer<V> valueSerializer, Jedisson jedisson) {
		super(name, jedisson);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	public IJedissonSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public IJedissonSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	
}
