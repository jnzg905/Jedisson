package org.jedisson.map;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.api.map.IJedissonAsyncMap;
import org.jedisson.api.map.IJedissonMap;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public class JedissonHashMap<K,V> extends AbstractJedissonMap<K,V> implements IJedissonMap<K,V>{
	
	private IJedissonAsyncMap<K,V> asyncMap;
	
	public JedissonHashMap(String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer valueSerializer,
			Jedisson jedisson) {
		super(name, keySerializer, valueSerializer, jedisson);
		asyncMap = jedisson.getAsyncMap(name, keySerializer,valueSerializer);
	}

	@Override
	public int size() {
		return asyncMap.size().join().intValue();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(final Object key) {
		return asyncMap.containsKey((K) key).join();
	}

	@Override
	public boolean containsValue(Object value) {
		return asyncMap.containsValue((V) value).join();
	}

	@Override
	public V get(final Object key) {
		return asyncMap.get((K) key).join();
	}

	@Override
	public V put(K key, V value) {
		return asyncMap.put(key, value).join();
	}

	@Override
	public V remove(Object key) {
		return asyncMap.remove((K) key).join();
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> m) {
		asyncMap.putAll(m).join();
	}

	@Override
	public void clear() {
		asyncMap.clear().join();
	}

	@Override
	public Set<K> keySet() {
		return new KeySet();
	}

	@Override
	public Collection<V> values() {
		return asyncMap.values().join();
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}

	@Override
	public void fastPut(final K key, final V value) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}
		asyncMap.fastPut(key, value).join();
	}
	
	public void fastRemove(final K... keys){
		asyncMap.fastRemove(keys).join();
	}
	
	protected Iterator<Entry<K,V>> newEntryIterator(){
		RedisConnection connection = getJedisson().getExecutor().getConnectionFactory().getConnection();
		return new EntryIterator(connection.hScan(getName().getBytes(), ScanOptions.scanOptions().build()),connection);
	}
	
	protected Iterator<K> newKeyIterator() {
		RedisConnection connection = getJedisson().getExecutor().getConnectionFactory().getConnection();
		return new KeyIterator(connection.hScan(getName().getBytes(), ScanOptions.scanOptions().build()),connection);
    }

	final class EntrySet extends AbstractSet<Entry<K,V>>{

		@Override
		public Iterator<java.util.Map.Entry<K, V>> iterator() {
			return newEntryIterator();
		}

		@Override
        public boolean contains(Object o) {
            return JedissonHashMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            JedissonHashMap.this.fastRemove((K)o);
            return true;
        }

        @Override
        public int size() {
            return JedissonHashMap.this.size();
        }

        @Override
        public void clear() {
        	JedissonHashMap.this.clear();
        }
		
	}
	
	final class KeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return newKeyIterator();
        }

        @Override
        public boolean contains(Object o) {
            return JedissonHashMap.this.containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            JedissonHashMap.this.fastRemove((K)o);
            return true;
        }

        @Override
        public int size() {
            return JedissonHashMap.this.size();
        }

        @Override
        public void clear() {
        	JedissonHashMap.this.clear();
        }
    }
	
	private class KeyIterator implements Iterator<K> {
		private RedisConnection connection;
		
		private Cursor<Map.Entry<byte[],byte[]>> cursor;
		private K curr;

		public KeyIterator(final Cursor<Map.Entry<byte[],byte[]>> cursor, RedisConnection connection) {
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
		public K next() {
			K k = (K) getKeySerializer().deserialize(cursor.next().getKey());
			curr = k;
			return k;
		}

		@Override
		public void remove() {
			JedissonHashMap.this.fastRemove(curr);
		}
	}
	 
	private class EntryIterator implements Iterator<Map.Entry<K, V>>{
		private RedisConnection connection;
		
		private Cursor<Map.Entry<byte[],byte[]>> cursor;
		
		private Map.Entry<K,V> curr;
		
		public EntryIterator(final Cursor<Map.Entry<byte[],byte[]>> cursor,RedisConnection connection){
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
		public java.util.Map.Entry<K, V> next() {
			Entry<byte[],byte[]> entry = cursor.next(); 
			curr = new HashEntry(getKeySerializer().deserialize(entry.getKey()),
					getValueSerializer().deserialize(entry.getValue()));
			return curr;
		}

		@Override
		public void remove() {
			JedissonHashMap.this.remove(curr.getKey());
		}
	}
	
	static class HashEntry<K,V> implements Map.Entry<K,V> {
		private K key;
		private V value;

		public HashEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}

		public V setValue(V value) {
			throw new UnsupportedOperationException();
		}
    }
}
