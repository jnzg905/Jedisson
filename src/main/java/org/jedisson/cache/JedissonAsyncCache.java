package org.jedisson.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncCache;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.async.JedissonCommand.DEL;
import org.jedisson.async.JedissonCommand.EVALSHA;
import org.jedisson.async.JedissonCommand.HDEL;
import org.jedisson.async.JedissonCommand.HEXISTS;
import org.jedisson.async.JedissonCommand.HGET;
import org.jedisson.async.JedissonCommand.HLEN;
import org.jedisson.async.JedissonCommand.HMGET;
import org.jedisson.async.JedissonCommand.HMSET;
import org.jedisson.async.JedissonCommand.HSET;
import org.jedisson.async.JedissonCommand.HSETNX;
import org.jedisson.util.JedissonUtil;
import org.jedisson.common.JedissonObject;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

public class JedissonAsyncCache<K,V> extends JedissonObject implements IJedissonAsyncCache<K,V>{

	private JedissonCacheConfiguration<K,V> configuration;
	
	protected CacheLoader<K,V> cacheLoader;
	
	protected CacheWriter<K,V> cacheWriter;
	
	public JedissonAsyncCache(String name,JedissonCacheConfiguration<K, V> configuration, Jedisson jedisson) {
		super(name, jedisson);
		this.configuration = (JedissonCacheConfiguration<K, V>) configuration;
		if(this.configuration.getKeySerializer() == null){
			this.configuration.setKeySerializer(JedissonUtil.newSerializer(
							getJedisson().getConfiguration().getKeySerializerType(),
							this.configuration.getKeyType()));
		}
		if(this.configuration.getValueSerializer() == null){
			this.configuration.setValueSerializer(JedissonUtil.newSerializer(
					getJedisson().getConfiguration().getValueSerializerType(), 
					this.configuration.getValueType()));
		}
		
		if(this.configuration.getCacheLoaderFactory() != null){
			cacheLoader = this.configuration.getCacheLoaderFactory().create();
		}
		if(this.configuration.getCacheWriterFactory() != null){
			cacheWriter = (CacheWriter<K, V>) this.configuration.getCacheWriterFactory().create();
		}
	}

	@Override
	public CompletableFuture<V> get(K key) {
		if (key == null) {
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException("map key can't be null");	
			});
				
		}
	
		final IJedissonSerializer<K> keySerializer = getConfiguration().getKeySerializer();
		final IJedissonSerializer valueSerializer = getConfiguration().getValueSerializer();
		Assert.notNull(keySerializer);
		Assert.notNull(valueSerializer);
		
		HGET<V> command = new HGET<>(valueSerializer,getName().getBytes(),keySerializer.serialize(key));
		return getJedisson().getAsyncService().execCommand(command).thenCompose(v -> { 
			if(v == null && cacheLoader != null){
				V value = cacheLoader.load(key);
				if(value != null){
					byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
					byte[] rawValue = getConfiguration().getValueSerializer().serialize(value);
					return getJedisson().getAsyncService().execCommand(new HSET(getName().getBytes(),rawKey,rawValue)).thenApply(t -> value);
				}else{
					return CompletableFuture.completedFuture(v);
				}
			}else{
				return CompletableFuture.completedFuture(v);
			}
		});
	}

	@Override
	public CompletableFuture<? extends Map<K, V>> getAll(Set<? extends K> keys) {
		if(keys == null || keys.contains(null)){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}
		
		byte[][] fields = new byte[keys.size()][];
		int i = 0;
		for(K key : keys){
			fields[i++] = getConfiguration().getKeySerializer().serialize(key);
		}
		HMGET command = new HMGET(getConfiguration().getValueSerializer(),getName().getBytes(),fields);
		return getJedisson().getAsyncService().execCommand(command).thenApply(v -> {
			int n = 0;
			List<V> results = (List<V>)v;
			Map<K,V> maps = new HashMap<>();
			for(K key : keys){
				maps.put(key,results.get(n++));
			}
			return maps;
		});
	}

	@Override
	public CompletableFuture<Long> put(K key, V value) {
		if (key == null) {
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException("map key can't be null");	
			});
				
		}
		if (value == null) {
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException("map value can't be null");	
			});
		}
		
		byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
		byte[] rawValue = getConfiguration().getValueSerializer().serialize(value);
		HSET command = new HSET(getName().getBytes(),rawKey,rawValue);
		return getJedisson().getAsyncService().execCommand(command).thenCompose(result -> {
			if(result != 0 && cacheWriter != null){
				cacheWriter.write(new JedissonCacheEntry<K,V>(key,value));
			}
			return CompletableFuture.completedFuture(result);
		});
				
	}

	@Override
	public CompletableFuture<V> getAndPut(K key, V value) {
		if (key == null) {
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException("map key can't be null");	
			});
			
		}
		if (value == null) {
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException("map value can't be null");	
			});
		}
		
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hset', KEYS[1], ARGV[1],ARGV[2]); " + 
				"return v;", 
				byte[].class);
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script, getConfiguration().getValueSerializer(), 
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getConfiguration().getKeySerializer().serialize(key),
					getConfiguration().getValueSerializer().serialize(value));
		}).thenApply(v -> {
			if(cacheWriter != null){
				cacheWriter.write(new JedissonCacheEntry<K,V>(key,value));
			}
			return (V) v;
		});
	}

	@Override
	public CompletableFuture<Boolean> containsKey(K key) {
		if(key == null){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}
		byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
		HEXISTS command = new HEXISTS(getName().getBytes(),rawKey);
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Boolean> putIfAbsent(K key, V value) {
		if(key == null || value == null){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}
		
		byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
		byte[] rawValue = getConfiguration().getValueSerializer().serialize(value);
		HSETNX command = new HSETNX(getName().getBytes(),rawKey,rawValue);
		return getJedisson().getAsyncService().execCommand(command).thenCompose(v -> {
			if(v && cacheWriter != null){
				cacheWriter.write(new JedissonCacheEntry<K,V>(key,value));	
			}
			return CompletableFuture.completedFuture(v);
		});
	}

	@Override
	public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> map) {
		if(map == null || map.containsKey(null)){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}

		final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(map.size());
		for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
			hashes.put(getConfiguration().getKeySerializer().serialize(entry.getKey()), 
					getConfiguration().getValueSerializer().serialize(entry.getValue()));
		}
		HMSET command = new HMSET(getName().getBytes(),hashes);
		return getJedisson().getAsyncService().execCommand(command).thenCompose(v -> {
			if(cacheWriter != null){
				List<Entry<? extends K, ? extends V>> valueList = new LinkedList<>();
				for(Map.Entry<? extends  K, ? extends V> entry : map.entrySet()){
					valueList.add(new JedissonCacheEntry<K,V>(entry.getKey(),entry.getValue()));
				}
				cacheWriter.writeAll(valueList);
			}
			return CompletableFuture.completedFuture(null);
		});
	}

	@Override
	public CompletableFuture<Long> remove(K key) {
		if(key == null){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}

		byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
		HDEL command = new HDEL(getName().getBytes(),rawKey);
		return getJedisson().getAsyncService().execCommand(command).thenCompose(v -> {
			if(v != 0 && cacheWriter != null){
				cacheWriter.delete(key);
			}
			return CompletableFuture.completedFuture(v);
		});
	}

	@Override
	public CompletableFuture<Long> removeAll(Set<? extends K> keys) {
		if(keys == null || keys.contains(null)){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}
		
		byte[][] fields = new byte[keys.size()][];
		int i = 0;
		for(K key : keys){
			fields[i++] = getConfiguration().getKeySerializer().serialize(key);
		}
		HDEL command = new HDEL(getName().getBytes(),fields);
		return getJedisson().getAsyncService().execCommand(command).thenCompose(v -> {
			if(v != 0 && cacheWriter != null){
				cacheWriter.deleteAll(keys);
			}	
			return CompletableFuture.completedFuture(v);
		});
	}

	@Override
	public CompletableFuture<V> getAndRemove(K key) {
		if(key == null){
			CompletableFuture.completedFuture(null).exceptionally(v -> {
				throw new NullPointerException();	
			});
		}
		
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v;", 
				byte[].class);
		
		byte[][] fields = new byte[2][];
		int i = 0;
		fields[i++] = getName().getBytes();
		fields[i++] = getConfiguration().getKeySerializer().serialize(key);		
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script, getConfiguration().getValueSerializer(), 
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getConfiguration().getKeySerializer().serialize(key));
		}).thenApply(v -> {
			if(cacheWriter != null){
				cacheWriter.delete(key);
			}
			return (V) v;
		});
	}

	@Override
	public CompletableFuture<Long> clear() {
		DEL command = new DEL(getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
		
	}

	@Override
	public JedissonCacheConfiguration<K, V> getConfiguration() {
		return configuration;
	}

	@Override
	public CompletableFuture<Long> removeAll() {
		return clear();
	}

	@Override
	public Iterator<Entry<K, V>> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CompletableFuture<Long> size() {
		HLEN command = new HLEN(getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
	}
}
