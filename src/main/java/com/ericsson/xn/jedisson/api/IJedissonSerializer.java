package com.ericsson.xn.jedisson.api;

import com.ericsson.xn.jedisson.common.JedssionSerializationException;

public interface IJedissonSerializer<T> {

	String serialize(T t) throws JedssionSerializationException;

	T deserialize(String str) throws JedssionSerializationException;
}
