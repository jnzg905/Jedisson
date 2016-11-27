package com.ericsson.xn.jedisson.serializer;

import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedssionSerializationException;

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
