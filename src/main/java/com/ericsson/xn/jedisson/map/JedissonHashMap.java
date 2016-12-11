package com.ericsson.xn.jedisson.map;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;

public class JedissonHashMap<K,V> extends AbstractJedissonMap<K,V>{
	
	public JedissonHashMap(String name, IJedissonSerializer<K> keySerializer, IJedissonSerializer<V> valueSerializer,
			Jedisson jedisson) {
		super(name, keySerializer,valueSerializer, jedisson);
	}

	@Override
	public int size() {
		return getJedisson().getRedisTemplate().opsForHash().size(getName()).intValue();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		return getJedisson().getRedisTemplate().opsForHash().hasKey(getName(), getKeySerializer().serialize((K) key));
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

		return getJedisson().getRedisTemplate().execute(script,
				Collections.<String>singletonList(getName()),
				getValueSerializer().serialize((V) value));
	}

	@Override
	public V get(Object key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}

		return (V) getValueSerializer().deserialize((String) getJedisson().getRedisTemplate().opsForHash().get(getName(), 
				getKeySerializer().serialize((K) key)));
	}

	@Override
	public V put(K key, V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}

		DefaultRedisScript<String> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
				"redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); " + 
				"return v", 
				String.class);
		
		return getValueSerializer().deserialize(getJedisson().getRedisTemplate().execute(
				script, 
				Collections.<String>singletonList(getName()), 
				getKeySerializer().serialize(key),getValueSerializer().serialize(value)));
	}

	@Override
	public V remove(Object key) {
		if (key == null) {
            throw new NullPointerException("map key can't be null");
        }
		DefaultRedisScript<String> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v", 
				String.class);
		return (V) getValueSerializer().deserialize(getJedisson().getRedisTemplate().execute(
				script, 
				Collections.<String>singletonList(getName()), 
				getKeySerializer().serialize((K) key)));
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
        Map<String,String> params = new HashMap<>();
        for(Map.Entry<? extends K,? extends V> entry : m.entrySet()){
        	params.put(getKeySerializer().serialize(entry.getKey()), getValueSerializer().serialize(entry.getValue()));
        }
        getJedisson().getRedisTemplate().opsForHash().putAll(getName(), params);
		
	}

	@Override
	public void clear() {
		getJedisson().getRedisTemplate().delete(getName());
	}

	@Override
	public Set<K> keySet() {
		return new KeySet();
	}

	@Override
	public Collection<V> values() {
		List<V> result = new LinkedList<>();
		List<String> values = getJedisson().getRedisTemplate().<String,String>opsForHash().values(getName());
		for(String value : values){
			result.add((V) getValueSerializer().deserialize(value));
		}
		return result;
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new EntrySet();
	}

	public void fastRemove(K... keys){
		List<String> params = new ArrayList<>();
		for(K key : keys){
			params.add(getKeySerializer().serialize(key));
		}
		getJedisson().getRedisTemplate().opsForHash().delete(getName(), params.toArray());
	}
	
	protected Iterator<Entry<K,V>> newEntryIterator(){
		return new EntryIterator(getJedisson().getRedisTemplate().<String,String>opsForHash().scan(getName(), ScanOptions.scanOptions().build()));
	}
	
	protected Iterator<K> newKeyIterator() {
       return new KeyIterator(getJedisson().getRedisTemplate().<String,String>opsForHash().scan(getName(), ScanOptions.scanOptions().build()));
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
		private Cursor<Map.Entry<String,String>> cursor;
		private K curr;

		public KeyIterator(final Cursor<Map.Entry<String,String>> cursor) {
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			return cursor.hasNext();
		}

		@Override
		public K next() {
			K k = getKeySerializer().deserialize(cursor.next().getKey());
			curr = k;
			return k;
		}

		@Override
		public void remove() {
			JedissonHashMap.this.remove(curr);
		}
	}
	 
	private class EntryIterator implements Iterator<Map.Entry<K, V>>{
		private Cursor<Map.Entry<String,String>> cursor;
		
		private Map.Entry<K,V> curr;
		
		public EntryIterator(final Cursor<Map.Entry<String,String>> cursor){
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			return cursor.hasNext();
		}

		@Override
		public java.util.Map.Entry<K, V> next() {
			Entry<String,String> entry = cursor.next(); 
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
