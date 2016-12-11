package org.jedisson.collection;

import java.util.List;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;

public abstract class AbstractJedissonCollection<V> extends JedissonObject implements List<V>{

	private final IJedissonSerializer<V> serializer;
	public AbstractJedissonCollection(String name, IJedissonSerializer<V> serializer,
			Jedisson jedisson) {
		super(name, jedisson);
		this.serializer = serializer;
	}
	public IJedissonSerializer<V> getSerializer() {
		return serializer;
	}

}
