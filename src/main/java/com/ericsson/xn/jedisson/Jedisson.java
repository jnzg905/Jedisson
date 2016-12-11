package com.ericsson.xn.jedisson;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;

import com.ericsson.xn.jedisson.api.IJedisson;
import com.ericsson.xn.jedisson.api.IJedissonPubSub;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.collection.JedissonList;
import com.ericsson.xn.jedisson.common.BeanLocator;
import com.ericsson.xn.jedisson.lock.JedissonLock;
import com.ericsson.xn.jedisson.map.JedissonHashMap;
import com.ericsson.xn.jedisson.pubsub.JedissonPubSub;

public class Jedisson implements IJedisson{	
	private final RedisTemplate<String,String> redisTemplate;
	
	protected Jedisson(final RedisTemplate<String,String> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}

	public static Jedisson getJedisson(){
		return getJedisson(BeanLocator.getBean(RedisTemplate.class));
	}
	
	public static Jedisson getJedisson(RedisTemplate<String,String> redisTemplate) {
		return new Jedisson(redisTemplate);
	}

	public RedisTemplate<String,String> getRedisTemplate(){
		return redisTemplate;
	}
	
	public <V> JedissonList<V> getList(final String name, IJedissonSerializer<V> serializer){
		return new JedissonList<V>(name,serializer,this);
	}

	@Override
	public <K, V> JedissonHashMap<K, V> getMap(String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer<V> valueSerializer) {
		return new JedissonHashMap<K,V>(name,keySerializer, valueSerializer,this);
	}

	@Override
	public IJedissonPubSub getPubSub(String name, IJedissonSerializer serializer) {
		return JedissonPubSub.getPubSub(name, serializer, this);
	}

	@Override
	public JedissonLock getLock(String name) {
		return JedissonLock.getLock(name, this);
	}
}
