package org.jedisson.util;

import java.lang.reflect.Constructor;

import org.jedisson.api.IJedissonSerializer;

public final class JedissonUtil {

	public static IJedissonSerializer newSerializer(String serializerClss, Class valueClss){
		try{
			if(valueClss == null){
				return (IJedissonSerializer) Class.forName(serializerClss).newInstance();
			}
			
			Constructor constructor = Class.forName(serializerClss).getConstructor(Class.class);
			return (IJedissonSerializer) constructor.newInstance(valueClss);	
		}catch(Exception e){
			throw new IllegalStateException(e);
		}
	}
}
