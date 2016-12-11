package org.jedisson;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.collection.JedissonList;
import org.jedisson.common.BeanLocator;
import org.jedisson.lock.JedissonLock;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.pubsub.JedissonPubSub;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;

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
