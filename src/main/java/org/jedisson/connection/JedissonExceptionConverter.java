package org.jedisson.connection;

import java.io.IOException;
import java.net.UnknownHostException;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.TooManyClusterRedirectionsException;

import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisRedirectionException;

public class JedissonExceptionConverter implements Converter<Exception, DataAccessException>{
	
	public DataAccessException convert(Exception ex) {

		if (ex instanceof DataAccessException) {
			return (DataAccessException) ex;
		}
		if (ex instanceof JedisDataException) {

			if (ex instanceof JedisRedirectionException) {
				JedisRedirectionException re = (JedisRedirectionException) ex;
				return new ClusterRedirectException(re.getSlot(), re.getTargetNode().getHost(), re.getTargetNode().getPort(),
						ex);
			}

			if (ex instanceof JedisClusterMaxRedirectionsException) {
				return new TooManyClusterRedirectionsException(ex.getMessage(), ex);
			}

			return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
		}
		if (ex instanceof JedisConnectionException) {
			return new RedisConnectionFailureException(ex.getMessage(), ex);
		}
		if (ex instanceof JedisException) {
			return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
		}
		if (ex instanceof UnknownHostException) {
			return new RedisConnectionFailureException("Unknown host " + ex.getMessage(), ex);
		}
		if (ex instanceof IOException) {
			return new RedisConnectionFailureException("Could not connect to Redis server", ex);
		}

		return null;
	}
}
