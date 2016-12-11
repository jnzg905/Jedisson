package com.ericsson.xn.jedisson.serializer;

import com.alibaba.fastjson.JSON;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedssionSerializationException;

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
