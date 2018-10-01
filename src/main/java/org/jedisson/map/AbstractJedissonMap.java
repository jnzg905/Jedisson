package org.jedisson.map;

import java.util.Map;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;

public abstract class AbstractJedissonMap<K,V> extends JedissonObject{
	
	private final IJedissonSerializer<K> keySerializer;
	
	private final IJedissonSerializer valueSerializer;
	
	public AbstractJedissonMap(String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer<V> valueSerializer, Jedisson jedisson) {
		super(name, jedisson);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	public IJedissonSerializer getValueSerializer() {
		return valueSerializer;
	}

	public IJedissonSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	
}
