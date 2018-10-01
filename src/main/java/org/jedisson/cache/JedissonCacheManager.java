package org.jedisson.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncCache;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonCacheManager;
import org.jedisson.common.JedissonObject;
import org.jedisson.lock.JedissonReentrantLock;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonJdkSerializer;
import org.jedisson.serializer.JedissonStringSerializer;

public class JedissonCacheManager extends JedissonObject implements IJedissonCacheManager{

	private JedissonHashMap<String,JedissonCacheConfiguration> cacheConfigMap;
	
	private JedissonReentrantLock lock;
	
	private Map<String,IJedissonAsyncCache> caches = new ConcurrentHashMap<>();
	
	public JedissonCacheManager(String name, Jedisson jedisson) {
		super("JedissonCacheManager:" + name, jedisson);
		
		lock = getJedisson().getReentrantLock(getName());
		cacheConfigMap = getJedisson().getMap(getName(), 
				new JedissonStringSerializer(),
				new JedissonJdkSerializer<JedissonCacheConfiguration>());
	}
		
	@Override
	public <K, V> IJedissonAsyncCache<K, V> getAsyncCache(String cacheName) {
		return getAsyncCache(cacheName,null);
	}

	@Override
	public <K, V> IJedissonAsyncCache<K, V> getAsyncCache(final String cacheName, JedissonCacheConfiguration<K, V> configuration) {
		if(cacheName == null){
			throw new NullPointerException();
		}
		
		IJedissonAsyncCache<K, V> cache = caches.get(cacheName);
		if (cache == null) {
			synchronized(caches){
				cache = caches.get(cacheName);
				if(cache == null){
					JedissonCacheConfiguration<K,V> jedissonCacheConfiguration = cacheConfigMap.get(cacheName);
					if(jedissonCacheConfiguration == null){
						if(configuration == null){
							return null;
						}
						try{
							lock.lock();
							jedissonCacheConfiguration = cacheConfigMap.get(cacheName);
							if(jedissonCacheConfiguration == null){
								cache = new JedissonAsyncCache<K,V>(cacheName, configuration, getJedisson());
								jedissonCacheConfiguration = cache.getConfiguration();
								cacheConfigMap.put(cacheName, jedissonCacheConfiguration);	
							}else{
								cache = new JedissonAsyncCache<K,V>(cacheName, jedissonCacheConfiguration, getJedisson());		
							}
							caches.put(cacheName, cache);
						}finally{
							lock.unlock();
						}
						
					}else{
						cache = new JedissonAsyncCache<K,V>(cacheName, jedissonCacheConfiguration, getJedisson());
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
		IJedissonAsyncCache cache = getAsyncCache(cacheName);
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
