package org.jedisson.cache;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.cache.Cache;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;
import javax.management.MBeanServer;

import org.jedisson.Jedisson;
import org.jedisson.common.JedissonObject;
import org.jedisson.lock.JedissonLock;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonJdkSerializer;
import org.jedisson.serializer.JedissonStringSerializer;

public class JedissonCacheManager extends JedissonObject implements javax.cache.CacheManager{
 
	private static MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
	
	private final URI uri;
	
	private final CachingProvider cachingProvider;
	
	private final ClassLoader classLoader;
	
	private Properties props = new Properties();
	
	private JedissonHashMap<String,JedissonCacheConfiguration> cacheConfigurations;
	
	private Map<String,JedissonCache> caches = new ConcurrentHashMap<>();
	
	private JedissonLock lock;
	
	public JedissonCacheManager(final URI uri, final JedissonCachingProvider cachingProvider, final ClassLoader classLoader, Properties props){
		super("JedissonCacheManager:" + classLoader.getClass().getSimpleName() + ":" + 
				uri.toString(),Jedisson.getJedisson());
		this.uri = uri;
		this.cachingProvider = cachingProvider;
		this.classLoader = classLoader;
		this.props = props == null ? new Properties() : props;
		
		lock = getJedisson().getLock(getName());
		
		cacheConfigurations = getJedisson().getMap(getName(), 
				new JedissonStringSerializer(),
				new JedissonJdkSerializer<JedissonCacheConfiguration>());
	}
	
	@Override
	public CachingProvider getCachingProvider() {
		return cachingProvider;
	}

	@Override
	public URI getURI() {
		return uri;
	}

	@Override
	public ClassLoader getClassLoader() {
		return classLoader;
	}

	@Override
	public Properties getProperties() {
		return props;
	}

	@Override
	public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(
			String cacheName, C configuration) throws IllegalArgumentException {
		JedissonCache<K,V> cache = null;
		JedissonCacheConfiguration<K,V> jedissonCacheConfiguration = cacheConfigurations.get(cacheName);
		if(jedissonCacheConfiguration == null){
			try{
				lock.lock();
				jedissonCacheConfiguration = cacheConfigurations.get(cacheName);
				if(jedissonCacheConfiguration == null){
					cache = new JedissonCache<K,V>(cacheName,configuration,getJedisson(), this);
					jedissonCacheConfiguration = cache.getConfiguration();
					cacheConfigurations.put(cacheName, jedissonCacheConfiguration);	
					caches.put(cacheName, cache);
				}else{
					cache = new JedissonCache<K,V>(cacheName,jedissonCacheConfiguration, getJedisson(), this);
					caches.put(cacheName, cache);
				}
			}finally{
				lock.unlock();
			}
		}else{
			throw new IllegalArgumentException("Cache:" + cacheName + " is already exist.");
		}
		return cache;
	}

	@Override
	public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
		if (cacheName == null || keyType == null || valueType == null) {
			throw new NullPointerException();
		}
		
		JedissonCache<K, V> cache = caches.get(cacheName);
		if (cache == null) {
			JedissonCacheConfiguration<K,V> configuration = cacheConfigurations.get(cacheName);
			if(configuration == null){
				return null;
			}
			synchronized(caches){
				cache = caches.get(cacheName);
				if(cache == null){
					cache = new JedissonCache<K,V>(cacheName,configuration,getJedisson(), this);
					caches.put(cacheName, cache);
				}
			}	
		}
		
		Class<?> actualKeyType = cache.getConfiguration(Configuration.class).getKeyType();
		Class<?> actualValueType = cache.getConfiguration(Configuration.class).getValueType();
		
		if (keyType != actualKeyType) {
			throw new ClassCastException("Cache has key type " + actualKeyType.getName()
					+ ", but getCache() called with key type " + keyType.getName());
		}
		
		if (valueType != actualValueType) {
			throw new ClassCastException("Cache has value type " + actualValueType.getName()
					+ ", but getCache() called with value type " + valueType.getName());
		}
		return null;
	}

	@Override
	public <K, V> Cache<K, V> getCache(String cacheName) {
		if (cacheName == null) {
			throw new NullPointerException();
		}
		
		JedissonCache<K, V> cache = caches.get(cacheName);
		if (cache == null) {
			JedissonCacheConfiguration<K,V> configuration = cacheConfigurations.get(cacheName);
			if(configuration == null){
				return null;
			}
			synchronized(caches){
				cache = caches.get(cacheName);
				if(cache == null){
					cache = new JedissonCache<K,V>(cacheName, configuration, getJedisson(), this);
					caches.put(cacheName, cache);
				}
			}
		}
		return cache;
	}

	@Override
	public Iterable<String> getCacheNames() {
		return Collections.unmodifiableList(new ArrayList<String>(caches.keySet()));
	}

	@Override
	public void destroyCache(String cacheName) {
		if (cacheName == null) {
			throw new NullPointerException();
		}
		Cache cache = getCache(cacheName);
		if(cache == null){
			return;
		}
		
		try{
			lock.lock();
			cache.clear();
			caches.remove(cache.getName());
			cacheConfigurations.fastRemove(cacheName);
		}finally{
			lock.unlock();
		}
	}

	@Override
	public void enableManagement(String cacheName, boolean enabled) {
		
	}

	@Override
	public void enableStatistics(String cacheName, boolean enabled) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		try{
			lock.lock();
			for(Cache cache : caches.values()){
				cache.clear();
			}
			caches.clear();
			cacheConfigurations.clear();
		}finally{
			lock.unlock();
		}
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		if (clazz == null) {
			throw new NullPointerException();
		}
		if(clazz.isAssignableFrom(this.getClass())){
			return clazz.cast(this);
		}
		
		throw new IllegalArgumentException("Cannot unwrap to " + clazz);
	} 
	
	void close(JedissonCache cache){
		try{
			lock.lock();
			caches.remove(cache.getName());
			cache.setClosed(true);
		}finally{
			lock.unlock();
		}
	}
}
