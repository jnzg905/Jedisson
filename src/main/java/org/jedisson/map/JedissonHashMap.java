package org.jedisson.map;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public class JedissonHashMap<K,V> extends AbstractJedissonMap<K,V>{
	
	public JedissonHashMap(String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer valueSerializer,
			Jedisson jedisson) {
		super(name, keySerializer, valueSerializer, jedisson);
	}

	@Override
	public int size() {
		return (int) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Integer>(){

			@Override
			public Integer doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.hLen(getName().getBytes()).intValue();
			}
			
		});
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(final Object key) {
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.hExists(getName().getBytes(),getKeySerializer().serialize((K) key));
			}
			
		});
	}

	@Override
	public boolean containsValue(Object value) {
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>(
				"local s = redis.call('hvals', KEYS[1]);" + 
				"for i = 1, #s, 1 do " + 
					"if ARGV[1] == s[i] then " + 
						"return 1 " + 
					"end " + 
				"end;" + 
				"return 0",Boolean.class);

		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				getValueSerializer().serialize((V) value));
	}

	@Override
	public V get(final Object key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}

		return (V) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<V>(){

			@Override
			public V doInRedis(RedisConnection connection)
					throws DataAccessException {
				return (V) getValueSerializer().deserialize(connection.hGet(getName().getBytes(), getKeySerializer().serialize((K) key)));
			}
			
		});
	}

	@Override
	public V put(K key, V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}

		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
				"redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); " + 
				"return v", 
				byte[].class);
		
		return (V) getJedisson().getConfiguration().getExecutor().execute(
				script, 
				getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getKeySerializer().serialize(key),
				getValueSerializer().serialize(value));
	}

	@Override
	public V remove(Object key) {
		if (key == null) {
            throw new NullPointerException("map key can't be null");
        }
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v", 
				byte[].class);
		return (V) getJedisson().getConfiguration().getExecutor().execute(
				script, 
				getValueSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getKeySerializer().serialize((K) key));
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> m) {
        getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(m.size());

				for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
					hashes.put(getKeySerializer().serialize(entry.getKey()), getValueSerializer().serialize(entry.getValue()));
				}
				connection.hMSet(getName().getBytes(), hashes);
				return null;
			}
        	
        });
	}

	@Override
	public void clear() {
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				connection.del(getName().getBytes());
				return null;
			}
			
		});
	}

	@Override
	public Set<K> keySet() {
		return new KeySet();
	}

	@Override
	public Collection<V> values() {
		return (Collection<V>) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Collection<V>>(){

			@Override
			public Collection<V> doInRedis(RedisConnection connection)
					throws DataAccessException {
				List<V> results = new ArrayList<>();
				List<byte[]> values = connection.hVals(getName().getBytes());
				for(int i = 0; i < values.size(); i++){
					results.add((V) getValueSerializer().deserialize(values.get(i)));
				}
				return results;
			}
			
		});
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}

	public void fastRemove(final K... keys){
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[][] hKeys = new byte[keys.length][];
				int i = 0;
				for(K key : keys){
					hKeys[i++] = getKeySerializer().serialize(key);
				}
				connection.hDel(getName().getBytes(), hKeys);
				return null;
			}
			
		});
	}
	
	protected Iterator<Entry<K,V>> newEntryIterator(){
		return new EntryIterator((Cursor<java.util.Map.Entry<byte[], byte[]>>) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Cursor<java.util.Map.Entry<byte[], byte[]>>>(){

			@Override
			public Cursor<java.util.Map.Entry<byte[], byte[]>> doInRedis(
					RedisConnection connection) throws DataAccessException {
				return connection.hScan(getName().getBytes(), ScanOptions.scanOptions().build());
			}
			
		}));
	}
	
	protected Iterator<K> newKeyIterator() {
       return new KeyIterator((Cursor<java.util.Map.Entry<byte[], byte[]>>) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Cursor<Map.Entry<byte[],byte[]>>>(){

		@Override
		public Cursor<java.util.Map.Entry<byte[], byte[]>> doInRedis(
				RedisConnection connection) throws DataAccessException {
			return connection.hScan(getName().getBytes(), ScanOptions.scanOptions().build());
		}
    	   
       }));
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
		private Cursor<Map.Entry<byte[],byte[]>> cursor;
		private K curr;

		public KeyIterator(final Cursor<Map.Entry<byte[],byte[]>> cursor) {
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			return cursor.hasNext();
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
		private Cursor<Map.Entry<byte[],byte[]>> cursor;
		
		private Map.Entry<K,V> curr;
		
		public EntryIterator(final Cursor<Map.Entry<byte[],byte[]>> cursor){
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			return cursor.hasNext();
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
