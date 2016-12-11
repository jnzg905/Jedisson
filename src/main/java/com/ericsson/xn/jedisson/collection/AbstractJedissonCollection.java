package com.ericsson.xn.jedisson.collection;

import java.util.List;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedissonObject;

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
