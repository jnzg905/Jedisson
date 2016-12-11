package org.jedisson.api;

import org.jedisson.common.JedssionSerializationException;

public interface IJedissonSerializer<T> {

	String serialize(T t) throws JedssionSerializationException;

	T deserialize(String str) throws JedssionSerializationException;
}
