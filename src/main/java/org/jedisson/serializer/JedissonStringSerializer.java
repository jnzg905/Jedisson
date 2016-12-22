package org.jedisson.serializer;

import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class JedissonStringSerializer extends JedissonAbstractSerializer<String>{

	public JedissonStringSerializer(){
		this(null);
	}
	
	public JedissonStringSerializer(Class<String> clss) {
		super(clss);
		// TODO Auto-generated constructor stub
	}

	@Override
	public byte[] serialize(String t) throws SerializationException {
		return t.getBytes();
	}

	@Override
	public String deserialize(byte[] str) throws SerializationException {
		return deserialize(str,null);
	}

	@Override
	public String deserialize(byte[] bytes, Class<String> clss)
			throws SerializationException {
		return new String(bytes);
	}

}
