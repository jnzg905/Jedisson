package org.jedisson.util;

import java.lang.reflect.Constructor;

import org.jedisson.api.IJedissonSerializer;

public final class JedissonUtil {

	public static <T> IJedissonSerializer<T> newSerializer(String serializerClss, Class<T> valueClss){
		try{
			if(valueClss == null){
				return (IJedissonSerializer<T>) Class.forName(serializerClss).newInstance();
			}
			
			Constructor constructor = Class.forName(serializerClss).getConstructor(Class.class);
			return (IJedissonSerializer<T>) constructor.newInstance(valueClss);	
		}catch(Exception e){
			throw new IllegalStateException(e);
		}
	}
}
