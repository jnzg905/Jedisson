package org.jedisson.serializer;

import java.io.Serializable;

import org.jedisson.api.IJedissonSerializer;

public abstract class JedissonAbstractSerializer<T> implements IJedissonSerializer<T>, Serializable{

	private final Class<T> clss;

	public JedissonAbstractSerializer(Class<T> clss){
		this.clss = clss;
	}

	public Class<T> getClss() {
		return clss;
	}
	
}
