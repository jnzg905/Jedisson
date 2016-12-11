package org.jedisson.serializer;

import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedssionSerializationException;

public class JedissonStringSerializer implements IJedissonSerializer<String>{

	@Override
	public String serialize(String t) throws JedssionSerializationException {
		return t;
	}

	@Override
	public String deserialize(String str) throws JedssionSerializationException {
		return str;
	}

}
