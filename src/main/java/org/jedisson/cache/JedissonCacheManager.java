package org.jedisson.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonCacheConfiguration;
import org.jedisson.api.IJedissonCacheManager;
import org.jedisson.common.JedissonObject;
import org.jedisson.lock.JedissonReentrantLock;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonJdkSerializer;
import org.jedisson.serializer.JedissonStringSerializer;

public class JedissonCacheManager extends JedissonObject implements IJedissonCacheManager{

	private JedissonHashMap<String,IJedissonCacheConfiguration> cacheConfigMap;
	
	private JedissonReentrantLock lock;
	
	private Map<String,IJedissonCache> caches = new ConcurrentHashMap<>();
	
	public JedissonCacheManager(String name, Jedisson jedisson) {
		super("JedissonCacheManager:" + name, jedisson);
		
		lock = getJedisson().getReentrantLock(getName());
		cacheConfigMap = getJedisson().getMap(getName(), 
				String.class,
				IJedissonCacheConfiguration.class,
				new JedissonStringSerializer(),
				new JedissonJdkSerializer<IJedissonCacheConfiguration>());
	}
	
	@Override
	public <K, V> IJedissonCache<K, V> getCache(String cacheName) {
		return getCache(cacheName,null);
	}

	@Override
	public <K, V> IJedissonCache<K, V> getCache(final String cacheName, IJedissonCacheConfiguration<K, V> configuration) {
		if(cacheName == null){
			throw new NullPointerException();
		}
		
		IJedissonCache<K, V> cache = caches.get(cacheName);
		if (cache == null) {
			synchronized(caches){
				cache = caches.get(cacheName);
				if(cache == null){
					IJedissonCacheConfiguration<K,V> jedissonCacheConfiguration = cacheConfigMap.get(cacheName);
					if(jedissonCacheConfiguration == null){
						if(configuration == null){
							return null;
						}
						try{
							lock.lock();
							jedissonCacheConfiguration = cacheConfigMap.get(cacheName);
							if(jedissonCacheConfiguration == null){
								cache = new JedissonCache<K,V>(cacheName, configuration, getJedisson());
								jedissonCacheConfiguration = cache.getConfiguration();
								cacheConfigMap.put(cacheName, jedissonCacheConfiguration);	
							}else{
								cache = new JedissonCache<K,V>(cacheName, jedissonCacheConfiguration, getJedisson());		
							}
							caches.put(cacheName, cache);
						}finally{
							lock.unlock();
						}
						
					}else{
						cache = new JedissonCache<K,V>(cacheName, jedissonCacheConfiguration, getJedisson());
						caches.put(cacheName, cache);
					}
				}
			}
		}
		return cache;
		
	}
	
	@Override
	public void removeCache(String cacheName) {
		if (cacheName == null) {
			throw new NullPointerException();
		}
		IJedissonCache cache = getCache(cacheName);
		if(cache == null){
			return;
		}
		
		try{
			lock.lock();
			cache.clear();
			caches.remove(cacheName);
			cacheConfigMap.fastRemove(cacheName);
		}finally{
			lock.unlock();
		}
	}

}
