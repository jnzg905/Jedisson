package org.jedisson;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonBlockingQueue;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonCacheConfiguration;
import org.jedisson.api.IJedissonCacheManager;
import org.jedisson.api.IJedissonConfiguration;
import org.jedisson.api.IJedissonList;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.blockingqueue.JedissonBlockingQueue;
import org.jedisson.cache.JedissonCache;
import org.jedisson.collection.JedissonList;
import org.jedisson.common.BeanLocator;
import org.jedisson.lock.JedissonLock;
import org.jedisson.lock.JedissonReentrantLock;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.pubsub.JedissonPubSub;
import org.jedisson.util.JedissonUtil;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

public class Jedisson implements IJedisson{	
	private static final String cacheManagerName = "DEFAULT_CACHEMANAGER";
	
	private volatile static Jedisson jedisson;
	
	private IJedissonConfiguration jedissonConfiguration;
	
	private IJedissonCacheManager cacheManager;
	
	protected Jedisson(final IJedissonConfiguration configuration) {
		jedissonConfiguration = configuration;
		cacheManager = newCacheManager(configuration.getCacheManagerType());
	}

	public static Jedisson getJedisson(){
		return getJedisson(BeanLocator.getBean(IJedissonConfiguration.class));
	}
	
	public static Jedisson getJedisson(IJedissonConfiguration configuration) {
		if(jedisson == null){
			synchronized(Jedisson.class){
				if(jedisson == null){
					jedisson = new Jedisson(configuration);			
				}
			}
		}
		return jedisson;
	}

	public IJedissonConfiguration getConfiguration(){
		return jedissonConfiguration;
	}
	
	public <V> IJedissonList<V> getList(final String name, Class<V> clss){
		return getList(name,clss,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),clss));
	}
	
	public <V> IJedissonList<V> getList(final String name, Class<V> clss, IJedissonSerializer serializer){
		return new JedissonList(name,clss, serializer,this);
	}

	@Override
	public <K, V> JedissonHashMap<K, V> getMap(String name, Class<K> keyClss, Class<V> valueClss) {
		return getMap(
				name,
				keyClss,
				valueClss,
				JedissonUtil.newSerializer(getConfiguration().getKeySerializerType(),keyClss),
				JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),valueClss));
	}
	
	@Override
	public <K, V> JedissonHashMap<K, V> getMap(String name, Class<K> keyClass, Class<V> valueClass,
			IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer) {
		return new JedissonHashMap<K,V>(name,keySerializer, valueSerializer,this);
	}

	@Override
	public IJedissonPubSub getPubSub(String name) {
		return getPubSub(name, JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),null));
	}

	@Override
	public IJedissonPubSub getPubSub(String name, IJedissonSerializer serializer) {
		return JedissonPubSub.getPubSub(name, serializer, this);
	}

	@Override
	public JedissonLock getLock(String name) {
		return new JedissonLock(name, this);
	}

	@Override
	public JedissonReentrantLock getReentrantLock(String name) {
		return new JedissonReentrantLock(name,this);
	}

	@Override
	public <K, V> IJedissonCache<K, V> getCache(String name) {
		return getCache(name, null);
	}
	
	@Override
	public <K, V> IJedissonCache<K, V> getCache(String name, IJedissonCacheConfiguration<K, V> cacheConfiguration) {
		return cacheManager.getCache(name,cacheConfiguration);
	}
	
	private IJedissonCacheManager newCacheManager(String cacheManagerType){
		try{
			Constructor constructor = Class.forName(cacheManagerType).getConstructor(String.class,Jedisson.class);
			return (IJedissonCacheManager)constructor.newInstance(cacheManagerName, this);	
		}catch(Exception e){
			throw new IllegalStateException(e);
		}
	}

	@Override
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(String name, Class<V> valueClss) {
		return getBlockingQueue(name,valueClss,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(), valueClss));
	}

	@Override
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(String name, Class<V> valueClss, IJedissonSerializer serializer) {
		return new JedissonBlockingQueue(name,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(), valueClss), this);
	}
}
