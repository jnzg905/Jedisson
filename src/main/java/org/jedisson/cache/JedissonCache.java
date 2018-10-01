package org.jedisson.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncCache;
import org.jedisson.api.IJedissonCache;
import org.jedisson.common.JedissonObject;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

public class JedissonCache<K,V> extends JedissonObject implements IJedissonCache<K,V>{

	private IJedissonAsyncCache<K,V> asyncCache;
	
	public JedissonCache(String name, JedissonCacheConfiguration<K,V> configuration, Jedisson jedisson) {
		super(name, jedisson);
		asyncCache = jedisson.getAsyncCache(name, configuration);
	}

	public JedissonCacheConfiguration<K, V> getConfiguration() {
		return (JedissonCacheConfiguration<K, V>) asyncCache.getConfiguration();
	}

	@Override
	public V get(final K key) {
		return asyncCache.get(key).join();
	}

	@Override
	public Map<K, V> getAll(final Set<? extends K> keys) {
		return asyncCache.getAll(keys).join();
	}

	@Override
	public void put(final K key, final V value) {
		asyncCache.put(key, value).join();		
	}

	@Override
	public V getAndPut(final K key, final V value) {
		return asyncCache.getAndPut(key, value).join();
	}

	@Override
	public boolean containsKey(final K key) {
		return asyncCache.containsKey(key).join();
	}

	@Override
	public boolean putIfAbsent(final K key, final V value) {
		return asyncCache.putIfAbsent(key, value).join();
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> map) {
		asyncCache.putAll(map).join();
	}

	@Override
	public boolean remove(final K key) {
		return asyncCache.remove(key).join().longValue() != 0;
	}

	@Override
	public void removeAll(final Set<? extends K> keys) {
		asyncCache.removeAll(keys).join();
	}

	@Override
	public void removeAll() {
		clear();
	}

	@Override
	public V getAndRemove(final K key) {
		return asyncCache.getAndRemove(key).join();
	}

	@Override
	public void clear() {
		asyncCache.clear().join();
	}

	@Override
	public Iterator<Cache.Entry<K, V>> iterator() {
		RedisConnection connection = getJedisson().getExecutor().getConnectionFactory().getConnection();
		return new EntryIterator(connection.hScan(getName().getBytes(), ScanOptions.scanOptions().build()),connection);
	}

	@Override
	public long size() {
		return asyncCache.size().join();
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
			curr = new JedissonCacheEntry(getConfiguration().getKeySerializer().deserialize(entry.getKey()),
					getConfiguration().getValueSerializer().deserialize(entry.getValue()));
			return curr;
		}

		@Override
		public void remove() {
			JedissonCache.this.remove(curr.getKey());
		}
	}
}
