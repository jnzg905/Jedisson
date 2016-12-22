package org.jedisson.serializer;

import java.io.Serializable;

import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import com.alibaba.fastjson.JSON;

public class JedissonFastJsonSerializer<T> extends JedissonAbstractSerializer<T>{
 
	public JedissonFastJsonSerializer(){
		this(null);
	}
	
	public JedissonFastJsonSerializer(Class<T> clss){
		super(clss);
	}
	
	@Override
	public byte[] serialize(T t) throws SerializationException {
		return t != null ? JSON.toJSONBytes(t): null;
	}

	@Override
	public T deserialize(byte[] str) throws SerializationException {
		if(getClss() == null){
			throw new SerializationException("clss is null.");
		}
		return (T) (str != null ? JSON.parseObject(str,getClss()) : null);
	}

	@Override
	public T deserialize(byte[] bytes, Class<T> clss)
			throws SerializationException {
		return (T) (bytes == null ? null : JSON.parseObject(bytes,clss));
	}
}
