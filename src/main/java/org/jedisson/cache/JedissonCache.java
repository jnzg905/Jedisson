package org.jedisson.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.junit.Assert;
import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonCache;
import org.jedisson.api.IJedissonCacheConfiguration;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.jedisson.util.JedissonUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public class JedissonCache<K,V> extends JedissonObject implements IJedissonCache<K,V>{

	private JedissonCacheConfiguration<K,V> configuration;
	
	private CacheLoader<K,V> cacheLoader;
	
	private CacheWriter<K,V> cacheWriter;
	
	public JedissonCache(String name, IJedissonCacheConfiguration<K,V> configuration, Jedisson jedisson) {
		super("JedissonCache:" + name, jedisson);
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

	public JedissonCacheConfiguration<K, V> getConfiguration() {
		return configuration;
	}

	@Override
	public V get(final K key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
	
		final IJedissonSerializer<K> keySerializer = configuration.getKeySerializer();
		final IJedissonSerializer<V> valueSerializer = configuration.getValueSerializer();
		Assert.assertNotNull(keySerializer);
		Assert.assertNotNull(valueSerializer);
		
		return (V) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<V>(){

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
	public Map<K, V> getAll(final Set<? extends K> keys) {	
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}	
		return (Map<K, V>) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Map<K,V>>(){

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
	public void put(final K key, final V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

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
	public V getAndPut(final K key, final V value) {
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
		
		V v = (V) getJedisson().getConfiguration().getExecutor().execute(
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
	public boolean containsKey(final K key) {
		if(key == null){
			throw new NullPointerException();
		}
		
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.hExists(getName().getBytes(), configuration.getKeySerializer().serialize(key));
			}
			
		});
	}

	@Override
	public boolean putIfAbsent(final K key, final V value) {
		if(key == null || value == null){
			throw new NullPointerException();
		}
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

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
	public void putAll(final Map<? extends K, ? extends V> map) {
		if(map == null || map.containsKey(null)){
			throw new NullPointerException();
		}
		
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

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
	public boolean remove(final K key) {
		if(key == null){
			throw new NullPointerException();
		}
		boolean ret = (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

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
		return ret;
	}

	@Override
	public void removeAll(final Set<? extends K> keys) {
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}
		
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

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
	public V getAndRemove(final K key) {
		if(key == null){
			throw new NullPointerException();
		}
		
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v;", 
				byte[].class);
		V v = (V) getJedisson().getConfiguration().getExecutor().execute(
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
	public void clear() {
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.del(getName().getBytes());
			}
		});
	}

	@Override
	public Iterator<Cache.Entry<K, V>> iterator() {
		RedisConnection connection = getJedisson().getConfiguration().getExecutor().getConnectionFactory().getConnection();
		return new EntryIterator(connection.hScan(getName().getBytes(), ScanOptions.scanOptions().build()),connection);
	}

	@Override
	public long size() {
		return (long) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Long>(){

			@Override
			public Long doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.hLen(getName().getBytes());
			}
			
		});
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
			JedissonCache.this.remove(curr.getKey());
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
}
