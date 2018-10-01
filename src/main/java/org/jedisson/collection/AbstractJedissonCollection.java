package org.jedisson.collection;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;

public abstract class AbstractJedissonCollection<V> extends JedissonObject{

	private final IJedissonSerializer<?> serializer;
		
	public AbstractJedissonCollection(String name, IJedissonSerializer<?> serializer, Jedisson jedisson) {
		super(name, jedisson);
		this.serializer = serializer;
	}
	
	public IJedissonSerializer getSerializer() {
		return serializer;
	}
}
