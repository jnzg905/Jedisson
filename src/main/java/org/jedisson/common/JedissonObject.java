package org.jedisson.common;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonObject;
import org.jedisson.api.IJedissonSerializer;

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
