package org.jedisson;

import java.lang.reflect.Constructor;

import org.jedisson.api.IBuilder;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonAsyncCache;
import org.jedisson.api.IJedissonAsyncPubSub;
import org.jedisson.api.IJedissonBlockingQueue;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonCacheManager;
import org.jedisson.api.IJedissonList;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonRedisExecutor;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.api.collection.IJedissonAsyncList;
import org.jedisson.async.JedissonAsyncService;
import org.jedisson.blockingqueue.JedissonBlockingQueue;
import org.jedisson.cache.JedissonCache;
import org.jedisson.cache.JedissonCacheConfiguration;
import org.jedisson.collection.JedissonAsyncList;
import org.jedisson.collection.JedissonList;
import org.jedisson.lock.JedissonLock;
import org.jedisson.lock.JedissonReentrantLock;
import org.jedisson.map.JedissonAsyncHashMap;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.pubsub.JedissonAsyncPubSub;
import org.jedisson.pubsub.JedissonPubSub;
import org.jedisson.util.JedissonUtil;
import org.springframework.data.redis.connection.RedisConnectionFactory;

public class Jedisson implements IJedisson{	
	private static final String cacheManagerName = "DEFAULT_CACHEMANAGER";
		
	private JedissonConfiguration jedissonConfiguration;
	
	private IJedissonCacheManager cacheManager;
	
	private JedissonAsyncService asyncService;
	
	private IJedissonRedisExecutor executor;
	
	protected Jedisson(final JedissonConfiguration configuration) {
		jedissonConfiguration = configuration;
		asyncService = new JedissonAsyncService(this);
		cacheManager = createCacheManager(configuration.getCacheManagerType());
		executor = createExecutor(configuration.getExecutor());
	}

	public static Builder builder(){
		return new Builder();
	}
	
	public static Builder builder(JedissonConfiguration config){
		return new Builder(config);
	}
	
	@Override
	public JedissonConfiguration getConfiguration(){
		return jedissonConfiguration;
	}
	
	public JedissonAsyncService getAsyncService() {
		return asyncService;
	}

	@Override
	public IJedissonRedisExecutor getExecutor(){
		return executor;
	}
	
	public <V> IJedissonList<V> getList(final String name, Class<V> clss){
		return getList(name,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),clss));
	}
	
	public <V> IJedissonList<V> getList(final String name, IJedissonSerializer serializer){
		return new JedissonList(name,serializer,this);
	}

	
	@Override
	public <V> IJedissonAsyncList<V> getAsyncList(String name, Class<V> clss) {
		return getAsyncList(name,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(), clss));
	}

	@Override
	public <V> IJedissonAsyncList<V> getAsyncList(String name, IJedissonSerializer serializer) {
		return new JedissonAsyncList(name,serializer,this);
	}

	@Override
	public <K, V> JedissonHashMap<K, V> getMap(String name, Class<K> keyClss, Class<V> valueClss) {
		return getMap(
				name,
				JedissonUtil.newSerializer(getConfiguration().getKeySerializerType(),keyClss),
				JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),valueClss));
	}
	
	@Override
	public <K, V> JedissonHashMap<K, V> getMap(String name, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer) {
		return new JedissonHashMap<K,V>(name,keySerializer, valueSerializer,this);
	}

	@Override
	public <K, V> JedissonAsyncHashMap<K, V> getAsyncMap(String name, Class<K> keyClss, Class<V> valueClss) {
		return getAsyncMap(name,
				JedissonUtil.newSerializer(getConfiguration().getKeySerializerType(),keyClss),
				JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),valueClss));
	}

	@Override
	public <K, V> JedissonAsyncHashMap<K, V> getAsyncMap(String name, IJedissonSerializer keySerializer, IJedissonSerializer valueSerializer) {
		return new JedissonAsyncHashMap<K,V>(name,keySerializer,valueSerializer,this);
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
	public IJedissonAsyncPubSub getAsyncPubSub(String name) {
		return getAsyncPubSub(name,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(),null));
	}

	@Override
	public IJedissonAsyncPubSub getAsyncPubSub(String name, IJedissonSerializer serializer) {
		return JedissonAsyncPubSub.getPubSub(name,serializer,this);
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
	public <K, V> IJedissonCache<K, V> getCache(String name, JedissonCacheConfiguration<K, V> cacheConfiguration) {
		return new JedissonCache<>(name,cacheConfiguration,this);
	}
	
	@Override
	public <K, V> IJedissonAsyncCache<K, V> getAsyncCache(String name) {
		return getAsyncCache(name,null);
	}

	@Override
	public <K, V> IJedissonAsyncCache<K, V> getAsyncCache(String name,JedissonCacheConfiguration<K, V> cacheConfiguration) {
		return cacheManager.getAsyncCache(name,cacheConfiguration);
	}

	private IJedissonCacheManager createCacheManager(String cacheManagerType){
		try{
			Constructor constructor = Class.forName(cacheManagerType).getConstructor(String.class,Jedisson.class);
			return (IJedissonCacheManager)constructor.newInstance(cacheManagerName, this);	
		}catch(Exception e){
			throw new IllegalStateException(e);
		}
	}
	
	private IJedissonRedisExecutor createExecutor(String executorType){
		try{
			Constructor constructor = Class.forName(executorType).getConstructor(RedisConnectionFactory.class);
			return (IJedissonRedisExecutor)constructor.newInstance(jedissonConfiguration.getRedisConnectionFactory());	
		}catch(Exception e){
			throw new IllegalStateException(e);
		}
	}

	@Override
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(String name, Class<V> valueClss) {
		return getBlockingQueue(name,JedissonUtil.newSerializer(getConfiguration().getValueSerializerType(), valueClss));
	}

	@Override
	public <V> IJedissonBlockingQueue<V> getBlockingQueue(String name, IJedissonSerializer serializer) {
		return new JedissonBlockingQueue(name,serializer, this);
	}
	
	public static class Builder implements IBuilder<IJedisson>{

		private JedissonConfiguration config;
		
		protected Builder(){
			this(new JedissonConfiguration());
		}
		
		private Builder(JedissonConfiguration config){
			this.config = config;
		}
		
		public Builder withFlushThreadNum(int flushThreadNum){
			config.setFlushThreadNum(flushThreadNum);
			return this;
		}
		
		public Builder withFlushSize(int flushSize){
			config.setFlushSize(flushSize);
			return this;
		}
		
		public Builder withFlushFreq(int flushFreq){
			config.setFlushFreq(flushFreq);
			return this;
		}
		
		public Builder withKeySerializerType(String keyType){
			config.setKeySerializerType(keyType);
			return this;
		}
		
		public Builder withValueSerializerType(String valueType){
			config.setValueSerializerType(valueType);
			return this;
		}
		
		public Builder withCacheManagerType(String cacheManagerType){
			config.setCacheManagerType(cacheManagerType);
			return this;
		}
		
		public Builder withRedisExecutor(String executor){
			config.setExecutor(executor);
			return this;
		}
	
		public Builder withRedisConnectionFactory(RedisConnectionFactory connectionFactory){
			config.setRedisConnectionFactory(connectionFactory);
			return this;
		}
		
		@Override
		public IJedisson builder() {
			return new Jedisson(config);
		}
		
	}
}
