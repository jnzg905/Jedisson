package org.jedisson.jsr107.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonStringSerializer;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import com.google.common.collect.Maps;

public class JedissonJsr107Cache<K,V> extends JedissonObject implements Cache<K,V>{

	private JedissonJsr107CacheConfiguration<K,V> configuration;
		
	private final transient JedissonJsr107CacheManager cacheManager;
	
	private volatile boolean closed;
	
	private CacheLoader<K,V> cacheLoader;
	
	private CacheWriter<K,V> cacheWriter;
	
	public JedissonJsr107Cache(String name, Configuration<K,V> configuration, Jedisson jedisson,JedissonJsr107CacheManager cacheManager) {
		super(name, jedisson);
		this.cacheManager = cacheManager;
		closed = false;
		if(configuration instanceof CompleteConfiguration){
			this.configuration = new JedissonJsr107CacheConfiguration<K,V>(name, (CompleteConfiguration<K, V>) configuration);
		}else{
			this.configuration = new JedissonJsr107CacheConfiguration<K,V>(name, configuration);
		}
		
		if(this.configuration.getCacheLoaderFactory() != null){
			cacheLoader = this.configuration.getCacheLoaderFactory().create();
		}
		if(this.configuration.getCacheWriterFactory() != null){
			cacheWriter = (CacheWriter<K, V>) this.configuration.getCacheWriterFactory().create();
		}
	}

	@Override
	public V get(final K key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		final IJedissonSerializer<K> keySerializer = configuration.getKeySerializer();
		final IJedissonSerializer<V> valueSerializer = configuration.getValueSerializer();
		
		return (V) getJedisson().getExecutor().execute(new RedisCallback<V>(){

			@Override
			public V doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[] rawKey = keySerializer.serialize(key);
				byte[] rawValue = connection.hGet(getName().getBytes(), rawKey);
				V v = valueSerializer.deserialize(rawValue);
				if(v == null && cacheLoader != null){
					v = cacheLoader.load(key);
					if(v != null){
						connection.hSet(getName().getBytes(), rawKey, rawValue);
					}		
				}
				return v;
			}
			
		});
	}

	@Override
	public Map<K,V> getAll(final Set<? extends K> keys) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}	
		return (Map<K, V>) getJedisson().getExecutor().execute(new RedisCallback<Map<K,V>>(){

			@Override
			public Map<K, V> doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[][] fields = new byte[keys.size()][];
				int i = 0;
				for(K key : keys){
					fields[i++] = configuration.getKeySerializer().serialize(key);
				}
				List<byte[]> values = connection.hMGet(getName().getBytes(), fields);
				Map<K,V> results = new HashMap<>();
				i = 0;
				for(K key : keys){
					results.put(key,configuration.getValueSerializer().deserialize(values.get(i++)));
				}
				if(results.isEmpty() && cacheLoader != null){
					results = cacheLoader.loadAll(keys);
					final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(results.size());

					for (Map.Entry<? extends K, ? extends V> entry : results.entrySet()) {
						hashes.put(configuration.getKeySerializer().serialize(entry.getKey()), 
								configuration.getValueSerializer().serialize(entry.getValue()));
					}
					connection.hMSet(getName().getBytes(), hashes);
				}
				return results;
			}
			
		});
	}

	@Override
	public boolean containsKey(final K key) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null){
			throw new NullPointerException();
		}
		
		return (boolean) getJedisson().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.hExists(getName().getBytes(), configuration.getKeySerializer().serialize(key));
			}
			
		});
	}

	@Override
	public void loadAll(Set<? extends K> keys, boolean replaceExistingValues,
			CompletionListener completionListener) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(cacheLoader == null){
			return;
		}
		
		try{
			Map<K,V> loadedMap = cacheLoader.loadAll(keys);
			if(replaceExistingValues){
				putAll(loadedMap);
			}else{
				for(Map.Entry<K, V> entry : loadedMap.entrySet()){
					if(!containsKey(entry.getKey())){
						put(entry.getKey(), entry.getValue());
					}
				}
			}
			if(completionListener != null){
				completionListener.onCompletion();
			}
		}catch(Exception e){
			if(completionListener != null){
				completionListener.onException(e);
			}
		}
	}

	@Override
	public void put(final K key, final V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		
		getJedisson().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				connection.hSet(getName().getBytes(), configuration.getKeySerializer().serialize(key), 
						configuration.getValueSerializer().serialize(value));
				if(cacheWriter != null){
					cacheWriter.write(new JedissonCacheEntry(key,value));
				}		
				return null;
			}
			
		});
		
		
	}

	@Override
	public V getAndPut(K key, V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
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
		
		V v = (V) getJedisson().getExecutor().execute(
				script,
				configuration.getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value));
		
		if(cacheWriter != null){
			cacheWriter.write(new JedissonCacheEntry(key,value));
		}
		return v;
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> map) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(map == null || map.containsKey(null)){
			throw new NullPointerException();
		}
		
		getJedisson().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(map.size());

				for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
					hashes.put(configuration.getKeySerializer().serialize(entry.getKey()), 
							configuration.getValueSerializer().serialize(entry.getValue()));
				}
				connection.hMSet(getName().getBytes(), hashes);
				if(cacheWriter != null){
					List<Cache.Entry<? extends K, ? extends V>> valueList = new LinkedList<>();
					for(Map.Entry<? extends  K, ? extends V> entry : map.entrySet()){
						valueList.add(new JedissonCacheEntry(entry.getKey(),entry.getValue()));
					}
					cacheWriter.writeAll(valueList);
				}
				return null;
			}
			
		});
	}

	@Override
	public boolean putIfAbsent(final K key, final V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || value == null){
			throw new NullPointerException();
		}
		return (boolean) getJedisson().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				boolean ret = connection.hSetNX(getName().getBytes(), configuration.getKeySerializer().serialize(key), 
						configuration.getValueSerializer().serialize(value));
				if(ret && cacheWriter != null){
					cacheWriter.write(new JedissonCacheEntry(key,value));	
				}
				return ret;
			}
			
		});
	}

	@Override
	public boolean remove(final K key) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null){
			throw new NullPointerException();
		}
		return (boolean) getJedisson().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				long ret = connection.hDel(getName().getBytes(),configuration.getKeySerializer().serialize(key));
				if(ret != 0 && cacheWriter != null){
					cacheWriter.delete(key);
				}
				return ret == 0 ? false : true;
			}
			
		});
	}

	@Override
	public boolean remove(K key, V oldValue) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || oldValue == null){
			throw new NullPointerException();
		}
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>(
				"if (redis.call('hget', KEYS[1], ARGV[1]) == ARGV[2]) then " +
					"redis.call('hdel', KEYS[1], ARGV[1]); " + 
					"return 1;" + 
				"else " + 
					"return 0;" + 
				"end;", 
				Boolean.class);
		
		boolean ret = (boolean) getJedisson().getExecutor().execute(
				script,
				configuration.getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(oldValue));
		
		if(ret && cacheWriter != null){
			cacheWriter.delete(key);
		}
		return ret;
	}

	@Override
	public V getAndRemove(K key) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null){
			throw new NullPointerException();
		}
		
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v;", 
				byte[].class);
		V v = (V) getJedisson().getExecutor().execute(
				script,
				configuration.getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				configuration.getKeySerializer().serialize(key));
		
		if(cacheWriter != null){
			cacheWriter.delete(key);
		}
		return v;
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || oldValue == null || newValue == null){
			throw new NullPointerException();
		}
		
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>(
				"if (redis.call('hget', KEYS[1], ARGV[1]) == ARGV[2]) then " +
					"redis.call('hset', KEYS[1], ARGV[1],ARGV[3]); " + 
					"return 1;" + 
				"else " + 
					"return 0;" + 
				"end;", 
				Boolean.class);
		boolean ret = (boolean) getJedisson().getExecutor().execute(
				script,
				configuration.getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(oldValue),
				configuration.getValueSerializer().serialize(newValue));
		
		if(ret && cacheWriter != null){
			cacheWriter.write(new JedissonCacheEntry(key,newValue));
		}
		return ret;
	}

	@Override
	public boolean replace(K key, V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || value == null){
			throw new NullPointerException();
		}
		
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>(
				"if (redis.call('hexists', KEYS[1], ARGV[1])) then " +
					"redis.call('hset', KEYS[1], ARGV[1],ARGV[2]); " + 
					"return 1;" + 
				"else " + 
					"return 0;" + 
				"end;", 
				Boolean.class);
		boolean ret = (boolean) getJedisson().getExecutor().execute(
				script,
				configuration.getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value));
		
		if(ret && cacheWriter != null){
			cacheWriter.write(new JedissonCacheEntry(key,value));
		}
		return ret;
	}

	@Override
	public V getAndReplace(K key, V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || value == null){
			throw new NullPointerException();
		}
		DefaultRedisScript<String> script = new DefaultRedisScript<>(
				"if (redis.call('hexists', KEYS[1], ARGV[1])) then " +
					"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
					"redis.call('hset', KEYS[1], ARGV[1],ARGV[2]); " + 
					"return v;" + 
				"else " + 
					"return nil;" + 
				"end;", 
				String.class);
		V v = (V) getJedisson().getExecutor().execute(
				script,
				configuration.getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value));
		
		if(v != null && cacheWriter != null){
			cacheWriter.write(new JedissonCacheEntry(key,value));
		}
		return v;
	}

	@Override
	public void removeAll(final Set<? extends K> keys) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}
		
		getJedisson().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[][] fields = new byte[keys.size()][];
				int i = 0;
				for(K key : keys){
					fields[i++] = configuration.getKeySerializer().serialize(key);
				}
				connection.hDel(getName().getBytes(), fields);
				if(cacheWriter != null){
					cacheWriter.deleteAll(keys);
				}		
				return null;
			}
		});
	}

	@Override
	public void removeAll() {
		clear();
	}

	@Override
	public void clear() {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		
		getJedisson().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.del(getName().getBytes());
			}
			
		});
	}

	public void setConfiguration(JedissonJsr107CacheConfiguration<K, V> configuration) {
		this.configuration = configuration;
	}

	public JedissonJsr107CacheConfiguration<K,V> getConfiguration(){
		return configuration;
	}
	
	@Override
	public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz){
		return clazz.cast(configuration);
	}
	
	@Override
	public <T> T invoke(K key,
            EntryProcessor<K, V, T> entryProcessor,
            Object... arguments) throws EntryProcessorException{
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || entryProcessor == null){
			throw new NullPointerException();
		}
		return null;
	}
	
	
	@Override
	public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
            EntryProcessor<K, V, T>
              entryProcessor,
            Object... arguments){
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		
		if(keys == null || keys.contains(null) || entryProcessor == null){
			throw new NullPointerException();
		}
		
		return null;
	}
	
	@Override
	public String getName() {
		return super.getName();
	}

	@Override
	public CacheManager getCacheManager() {
		return cacheManager;
	}

	@Override
	public void close() {
		cacheManager.close(this);
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	public void setClosed(boolean closed) {
		this.closed = closed;
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		return clazz.cast(this);
	}

	@Override
	public void registerCacheEntryListener(
			CacheEntryListenerConfiguration<K,V> cacheEntryListenerConfiguration) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(cacheEntryListenerConfiguration == null){
			throw new NullPointerException();
		}
	}

	@Override
	public void deregisterCacheEntryListener(
			CacheEntryListenerConfiguration<K,V> cacheEntryListenerConfiguration) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(cacheEntryListenerConfiguration == null){
			throw new NullPointerException();
		}
		
	}

	@Override
	public Iterator<Cache.Entry<K, V>> iterator() {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		RedisConnection connection = getJedisson().getExecutor().getConnectionFactory().getConnection();
		return new EntryIterator(connection.hScan(getName().getBytes(),ScanOptions.scanOptions().build()), connection);
	}
	
	class EntryIterator implements Iterator<Cache.Entry<K, V>>{
		private RedisConnection connection;
		
		private Cursor<Map.Entry<byte[],byte[]>> cursor;
		
		private Cache.Entry<K,V> curr;
		
		public EntryIterator(final Cursor<Map.Entry<byte[],byte[]>> cursor, RedisConnection connection){
			this.cursor = cursor;
			this.connection = connection;
		}

		@Override
		public boolean hasNext() {
			boolean ret = cursor.hasNext();
			if(!ret){
				connection.close();
			}
			return ret;
		}

		@Override
		public Cache.Entry<K, V> next() {
			Map.Entry<byte[],byte[]> entry = cursor.next(); 
			curr = new JedissonCacheEntry(configuration.getKeySerializer().deserialize(entry.getKey()),
					configuration.getValueSerializer().deserialize(entry.getValue()));
			return curr;
		}

		@Override
		public void remove() {
			JedissonJsr107Cache.this.remove(curr.getKey());
		}
	}
	
	class JedissonCacheEntry implements Cache.Entry<K,V> {
		private K key;
		private V value;

		public JedissonCacheEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public <T> T unwrap(Class<T> clazz) {
			// TODO Auto-generated method stub
			return null;
		}
    } 
	
	class ValueHolder<V>{
		private long creationTime;
		
		private long accessTime;
		
		private long updateTime;
		
		private V v;
		
		public ValueHolder(V v){
			this.v = v;
		}

		public long getCreationTime() {
			return creationTime;
		}

		public void setCreationTime(long creationTime) {
			this.creationTime = creationTime;
		}

		public long getAccessTime() {
			return accessTime;
		}

		public void setAccessTime(long accessTime) {
			this.accessTime = accessTime;
		}

		public long getUpdateTime() {
			return updateTime;
		}

		public void setUpdateTime(long updateTime) {
			this.updateTime = updateTime;
		}

		public V getV() {
			return v;
		}	
	}
}
