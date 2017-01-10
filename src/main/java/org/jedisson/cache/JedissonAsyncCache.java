package org.jedisson.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonCacheConfiguration;
import org.jedisson.api.IJedissonPromise;
import org.jedisson.api.IJedissonPromise.IPromiseListener;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.async.JedissonCommand.DEL;
import org.jedisson.async.JedissonCommand.EVALSHA;
import org.jedisson.async.JedissonCommand.HDEL;
import org.jedisson.async.JedissonCommand.HEXISTS;
import org.jedisson.async.JedissonCommand.HGET;
import org.jedisson.async.JedissonCommand.HMGET;
import org.jedisson.async.JedissonCommand.HMSET;
import org.jedisson.async.JedissonCommand.HSET;
import org.jedisson.async.JedissonCommand.HSETNX;
import org.jedisson.async.JedissonPromise;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.Assert;

public class JedissonAsyncCache<K,V> extends JedissonCache<K,V>{

	private static final ThreadLocal<IJedissonPromise> currFuture = new ThreadLocal<>();
	
	public JedissonAsyncCache(String name,
			IJedissonCacheConfiguration<K, V> configuration, Jedisson jedisson) {
		super(name, configuration, jedisson);
		// TODO Auto-generated constructor stub
	}

	@Override
	public V get(K key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
	
		final IJedissonSerializer<K> keySerializer = getConfiguration().getKeySerializer();
		final IJedissonSerializer<V> valueSerializer = getConfiguration().getValueSerializer();
		Assert.notNull(keySerializer);
		Assert.notNull(valueSerializer);
		
		IJedissonPromise<V> promise = new JedissonPromise<>(valueSerializer);
		try{
			IJedissonPromise<V> internalPromise = new JedissonPromise<>(valueSerializer);
			HGET command = new HGET(internalPromise,getName().getBytes(),keySerializer.serialize(key));
			getJedisson().getAsyncService().sendCommand(command);
			internalPromise.onSuccess(new IPromiseListener<IJedissonPromise>(){
				
				@Override
				public IJedissonPromise apply(IJedissonPromise p) {
					try{
						V value = (V) p.get();	
						if(value == null && cacheLoader != null){
							value = cacheLoader.load(key);
							if(value != null){
								byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
								byte[] rawValue = getConfiguration().getValueSerializer().serialize(value);
								HSET command = new HSET(promise,getName().getBytes(),rawKey,rawValue);
								getJedisson().getAsyncService().sendCommand(command);
							}else{
								promise.setSuccess(value);
							}
						}else{
							promise.setSuccess(value);
						}
					}catch(Exception e){
						promise.setFailure(e);
					}
					return p;
				}
				
			});
		}catch(InterruptedException e){
			promise.setFailure(e);
		}
		currFuture.set(promise);
		return null;
	}

	@Override
	public Map<K, V> getAll(Set<? extends K> keys) {
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}
		
		IJedissonPromise<V> promise = new JedissonPromise<>(getConfiguration().getValueSerializer());
		try{
			byte[][] fields = new byte[keys.size()][];
			int i = 0;
			for(K key : keys){
				fields[i++] = getConfiguration().getKeySerializer().serialize(key);
			}
			HMGET command = new HMGET(promise,getName().getBytes(),fields);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			promise.setFailure(e);
		}
		currFuture.set(promise);
		return null;
	}

	@Override
	public void put(K key, V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		IJedissonPromise<V> promise = new JedissonPromise<>(getConfiguration().getValueSerializer());
		try{
			IJedissonPromise<V> internalPromise = new JedissonPromise<>(getConfiguration().getValueSerializer());
			byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
			byte[] rawValue = getConfiguration().getValueSerializer().serialize(value);
			HSET command = new HSET(internalPromise,getName().getBytes(),rawKey,rawValue);
			getJedisson().getAsyncService().sendCommand(command);
			internalPromise.onSuccess(new IPromiseListener<IJedissonPromise>(){

				@Override
				public IJedissonPromise apply(IJedissonPromise p) {
					try{
						Boolean result = (Boolean) p.get();	
						if(result && cacheWriter != null){
							cacheWriter.write(new JedissonCacheEntry(key,value));
						}	
						promise.setSuccess(result);
					}catch(Exception e){
						promise.setFailure(e);
					}
					return p;
				}
				
			});
		}catch(InterruptedException e){
			promise.setFailure(e);
		}
		currFuture.set(promise);
	}

	@Override
	public V getAndPut(K key, V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hset', KEYS[1], ARGV[1],ARGV[2]); " + 
				"return v;", 
				byte[].class);
		
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			byte[][] fields = new byte[3][];
			int i = 0;
			fields[i++] = getName().getBytes();
			fields[i++] = getConfiguration().getKeySerializer().serialize(key);
			fields[i++] = getConfiguration().getValueSerializer().serialize(value);
			EVALSHA command = new EVALSHA(future,script,1,fields);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public boolean containsKey(K key) {
		if(key == null){
			throw new NullPointerException();
		}
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
			HEXISTS command = new HEXISTS(future,getName().getBytes(),rawKey);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean putIfAbsent(K key, V value) {
		if(key == null || value == null){
			throw new NullPointerException();
		}
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
			byte[] rawValue = getConfiguration().getValueSerializer().serialize(value);
			HSETNX command = new HSETNX(future,getName().getBytes(),rawKey,rawValue);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		if(map == null || map.containsKey(null)){
			throw new NullPointerException();
		}
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(map.size());

			for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
				hashes.put(getConfiguration().getKeySerializer().serialize(entry.getKey()), 
						getConfiguration().getValueSerializer().serialize(entry.getValue()));
			}
			HMSET command = new HMSET(future,getName().getBytes(),hashes);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
	}

	@Override
	public boolean remove(K key) {
		if(key == null){
			throw new NullPointerException();
		}
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			byte[] rawKey = getConfiguration().getKeySerializer().serialize(key);
			HDEL command = new HDEL(future,getName().getBytes(),rawKey);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public void removeAll(Set<? extends K> keys) {
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			byte[][] fields = new byte[keys.size()][];
			int i = 0;
			for(K key : keys){
				fields[i++] = getConfiguration().getKeySerializer().serialize(key);
			}
			HDEL command = new HDEL(future,getName().getBytes(),fields);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
	}

	@Override
	public V getAndRemove(K key) {
		if(key == null){
			throw new NullPointerException();
		}
		
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v;", 
				byte[].class);
		
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			byte[][] fields = new byte[2][];
			int i = 0;
			fields[i++] = getName().getBytes();
			fields[i++] = getConfiguration().getKeySerializer().serialize(key);
			EVALSHA command = new EVALSHA(future,script,1,fields);
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public void clear() {
		IJedissonPromise future = new JedissonPromise(getConfiguration().getValueSerializer());
		try{
			DEL command = new DEL(future,getName().getBytes());
			getJedisson().getAsyncService().sendCommand(command);
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
	}

	@Override
	public IJedissonCache<K,V> withAsync() {
		return this;
	}

	@Override
	public boolean isAsync() {
		return true;
	}

	@Override
	public <R> IJedissonPromise<R> future() {
		return currFuture.get();
	}

}
