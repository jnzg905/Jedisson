package org.jedisson.collection;

import java.util.Collection;
import java.util.List;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;

public abstract class AbstractJedissonCollection<V> extends JedissonObject implements Collection<V>{

	private final IJedissonSerializer serializer;
	
	private final Class<V> clss;
	
	public AbstractJedissonCollection(String name, Class<V> clss, IJedissonSerializer serializer, Jedisson jedisson) {
		super(name, jedisson);
		this.clss = clss;
		this.serializer = serializer;
	}
	
	public Class<V> getClss() {
		return clss;
	}

	public IJedissonSerializer getSerializer() {
		return serializer;
	}
	
}
