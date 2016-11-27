package com.ericsson.xn.jedisson.common;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonObject;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;

public abstract class JedissonObject implements IJedissonObject{

	private final Jedisson jedisson;
	
	private final String name;
	
	private final IJedissonSerializer serializer;
	
	public JedissonObject(final String name,IJedissonSerializer serializer,final Jedisson jedisson){
		this.name = name;
		this.serializer = serializer;
		this.jedisson = jedisson;
	}

	public IJedissonSerializer getSerializer() {
		return serializer;
	}

	public String getName() {
		return name;
	}
	
	public Jedisson getJedisson(){
		return jedisson;
	}
}
