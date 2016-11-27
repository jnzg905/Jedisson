package com.ericsson.xn.jedisson.collection;

import java.util.List;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedissonObject;

public abstract class AbstractJedissonList<V> extends JedissonObject implements List<V>{

	public AbstractJedissonList(String name, IJedissonSerializer serializer,
			Jedisson jedisson) {
		super(name, serializer, jedisson);
		// TODO Auto-generated constructor stub
	}

}
