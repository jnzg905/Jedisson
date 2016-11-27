package com.ericsson.xn.jedisson;

import org.springframework.data.redis.core.RedisTemplate;

import com.ericsson.xn.jedisson.api.IJedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.collection.JedissonList;
import com.ericsson.xn.jedisson.map.JedissonHashMap;

public class Jedisson implements IJedisson{
	private final RedisTemplate redisTemplate;

	protected Jedisson(final RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	public static Jedisson getJedisson(RedisTemplate redisTemplate) {
		return new Jedisson(redisTemplate);
	}

	public RedisTemplate getRedisTemplate(){
		return redisTemplate;
	}
	
	public <V> JedissonList<V> getList(final String name, IJedissonSerializer serializer){
		return new JedissonList<V>(name,serializer,this);
	}

	@Override
	public <K, V> JedissonHashMap<K, V> getMap(String name, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer) {
		return new JedissonHashMap<K,V>(name,keySerializer, valueSerializer,this);
	}
}
