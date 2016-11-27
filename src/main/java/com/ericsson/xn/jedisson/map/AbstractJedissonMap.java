package com.ericsson.xn.jedisson.map;

import java.util.Map;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedissonObject;

public abstract class AbstractJedissonMap<K,V> extends JedissonObject implements Map<K,V>{

	public AbstractJedissonMap(String name, IJedissonSerializer serializer,
			Jedisson jedisson) {
		super(name, serializer, jedisson);
		// TODO Auto-generated constructor stub
	}

}
