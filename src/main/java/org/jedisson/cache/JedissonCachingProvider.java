package org.jedisson.cache;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;

import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedisson;
import org.jedisson.lock.JedissonLock;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonStringSerializer;
import org.springframework.data.redis.core.RedisTemplate;

public class JedissonCachingProvider implements javax.cache.spi.CachingProvider{

	private static final URI DEFAULT_URI = URI.create("jedisson://default");
		
	public static final Properties DFLT_PROPS = new Properties();
		
	private final Map<ClassLoader,Map<URI,CacheManager>> cacheManagers = new WeakHashMap<>();

	@Override
	public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
		if (uri == null)
            uri = getDefaultURI();
 
        if (classLoader == null)
        	classLoader = getDefaultClassLoader();
        
        Map<URI,CacheManager> uriMap = cacheManagers.get(classLoader);
        if(uriMap == null){
        	synchronized(cacheManagers){
        		uriMap = cacheManagers.get(classLoader);
        		if(uriMap == null){
        			uriMap = new HashMap<>();
        			cacheManagers.put(classLoader, uriMap);
        		}
        	}
        }
        	
        CacheManager cacheManager = uriMap.get(uri);
        if(cacheManager == null){
        	synchronized(uriMap){
        		cacheManager = uriMap.get(uri);
        		if(cacheManager == null){
        			cacheManager = new JedissonCacheManager(uri,this,classLoader,properties);
        			uriMap.put(uri, cacheManager);
        		}
        	}
        }
        return cacheManager;
	}

	@Override
	public ClassLoader getDefaultClassLoader() {
		return this.getClass().getClassLoader();
	}

	@Override
	public URI getDefaultURI() {
		return DEFAULT_URI;
	}

	@Override
	public Properties getDefaultProperties() {
		return new Properties();
	}

	@Override
	public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
		return getCacheManager(uri,classLoader,getDefaultProperties());
	}

	@Override
	public CacheManager getCacheManager() {
		return getCacheManager(getDefaultURI(),getDefaultClassLoader());
	}

	@Override
	public void close() {
		synchronized(cacheManagers){
			for(Map<URI,CacheManager> uriMap : cacheManagers.values()){
				for(CacheManager cacheManager : uriMap.values()){
					cacheManager.close();
				}
				uriMap.clear();
			}
			cacheManagers.clear();	
		}
	}

	@Override
	public void close(ClassLoader classLoader) {
		synchronized(cacheManagers){
			Map<URI,CacheManager> uriMap = cacheManagers.remove(classLoader);
			if(uriMap != null){ 
				for(CacheManager cacheManager : uriMap.values()){
						cacheManager.close();
				}
				uriMap.clear();
			}	
		}
	}

	@Override
	public void close(URI uri, ClassLoader classLoader) {
		synchronized(cacheManagers){
			Map<URI,CacheManager> uriMap = cacheManagers.get(classLoader);
			if(uriMap != null){
				CacheManager cacheManager = uriMap.remove(uri);
				if(cacheManager != null){
					cacheManager.close();	
				}
			}
		}
	}

	@Override
	public boolean isSupported(OptionalFeature optionalFeature) {
		// TODO Auto-generated method stub
		return false;
	}
}
