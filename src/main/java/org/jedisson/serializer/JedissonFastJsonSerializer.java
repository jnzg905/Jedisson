package org.jedisson.serializer;

import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedssionSerializationException;

import com.alibaba.fastjson.JSON;

public class JedissonFastJsonSerializer<T> implements IJedissonSerializer<T>{
 
	private Class<T> clss;
	
	public JedissonFastJsonSerializer(Class<T> clss){
		this.clss = clss;
	}
	
	@Override
	public String serialize(T t) throws JedssionSerializationException {
		return JSON.toJSONString(t);
	}

	@Override
	public T deserialize(String str) throws JedssionSerializationException {
		return JSON.parseObject(str,clss);
	}
}
