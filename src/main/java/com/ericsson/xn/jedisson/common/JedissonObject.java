package com.ericsson.xn.jedisson.common;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonObject;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;

public abstract class JedissonObject implements IJedissonObject{

	private final transient Jedisson jedisson;
	
	private final String name;

	public JedissonObject(final String name,final Jedisson jedisson){
		this.name = name;
		this.jedisson = jedisson;
	}
	
	public String getName() {
		return name;
	}
	
	public Jedisson getJedisson(){
		return jedisson;
	}
}
