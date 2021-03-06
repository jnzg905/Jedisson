package org.jedisson.connection;

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.ReturnType;

import redis.clients.util.SafeEncoder;

public class JedissonScriptReturnConverter implements Converter<Object, Object>{
	private final ReturnType returnType;

	public JedissonScriptReturnConverter(ReturnType returnType) {
		this.returnType = returnType;
	}

	@SuppressWarnings("unchecked")
	public Object convert(Object result) {
		if (result instanceof String) {
			// evalsha converts byte[] to String. Convert back for consistency
			return SafeEncoder.encode((String) result);
		}
		if (returnType == ReturnType.STATUS) {
			return JedissonConverters.toString((byte[]) result);
		}
		if (returnType == ReturnType.BOOLEAN) {
			// Lua false comes back as a null bulk reply
			if (result == null) {
				return Boolean.FALSE;
			}
			return ((Long) result == 1);
		}
		if (returnType == ReturnType.MULTI) {
			List<Object> resultList = (List<Object>) result;
			List<Object> convertedResults = new ArrayList<Object>();
			for (Object res : resultList) {
				if (res instanceof String) {
					// evalsha converts byte[] to String. Convert back for
					// consistency
					convertedResults.add(SafeEncoder.encode((String) res));
				} else {
					convertedResults.add(res);
				}
			}
			return convertedResults;
		}
		return result;
	}
}
