package org.jedisson.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.jedisson.map.JedissonHashMap;
import org.jedisson.serializer.JedissonStringSerializer;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import com.google.common.collect.Maps;

public class JedissonCache<K,V> extends JedissonObject implements Cache<K,V>{

	private JedissonCacheConfiguration<K,V> configuration;
		
	private final transient JedissonCacheManager cacheManager;
	
	private volatile boolean closed;
	
	public JedissonCache(String name, Configuration<K,V> configuration, Jedisson jedisson,JedissonCacheManager cacheManager) {
		super(name, jedisson);
		this.cacheManager = cacheManager;
		closed = false;
		if(configuration instanceof CompleteConfiguration){
			this.configuration = new JedissonCacheConfiguration<K,V>(name, (CompleteConfiguration<K, V>) configuration);
		}else{
			this.configuration = new JedissonCacheConfiguration<K,V>(name, configuration);
		}
	}

	@Override
	public V get(K key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		IJedissonSerializer<K> keySerializer = configuration.getKeySerializer();
		IJedissonSerializer<V> valueSerializer = configuration.getValueSerializer();
		return valueSerializer.deserialize((String) getJedisson().getRedisTemplate().opsForHash().get(getName(), keySerializer.serialize(key)));
	}

	@Override
	public Map<K,V> getAll(Set<? extends K> keys) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}
		
		Map<String,K> keyList = new HashMap<>();
		for(K key : keys){
			keyList.put(configuration.getKeySerializer().serialize(key), key);
		}
		
		List<String> values = getJedisson().getRedisTemplate().<String,String>opsForHash().multiGet(getName(), keyList.keySet());
		Map<K,V> results = new HashMap<>();
		int i = 0;
		for(String key : keyList.keySet()){
			results.put(keyList.get(key),configuration.getValueSerializer().deserialize(values.get(i++)));
		}
		return results;
	}

	@Override
	public boolean containsKey(K key) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null){
			throw new NullPointerException();
		}
		return getJedisson().getRedisTemplate().opsForHash().hasKey(getName(), configuration.getKeySerializer().serialize(key));
	}

	@Override
	public void loadAll(Set<? extends K> keys, boolean replaceExistingValues,
			CompletionListener completionListener) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		
	}

	@Override
	public void put(K key, V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		
		getJedisson().getRedisTemplate().opsForHash().put(getName(), configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value));		
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
		
		DefaultRedisScript<String> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hset', KEYS[1], ARGV[1],ARGV[2]); " + 
				"return v;", 
				String.class);
		return configuration.getValueSerializer().deserialize(getJedisson().getRedisTemplate().execute(
				script,
				Collections.<String>singletonList(getName()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value)));
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(map == null || map.containsKey(null)){
			throw new NullPointerException();
		}
		
		Map<String,String> params = new HashMap<>();
		for(Map.Entry<? extends K,? extends V> entry : map.entrySet()){
			params.put(configuration.getKeySerializer().serialize(entry.getKey()), 
					configuration.getValueSerializer().serialize(entry.getValue()));
		}
		getJedisson().getRedisTemplate().opsForHash().putAll(getName(), params);
		
	}

	@Override
	public boolean putIfAbsent(K key, V value) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null || value == null){
			throw new NullPointerException();
		}
		return getJedisson().getRedisTemplate().opsForHash().putIfAbsent(getName(), 
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value));
	}

	@Override
	public boolean remove(K key) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null){
			throw new NullPointerException();
		}
		getJedisson().getRedisTemplate().opsForHash().delete(getName(), configuration.getKeySerializer().serialize(key));
		return true;
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
		return getJedisson().getRedisTemplate().execute(
				script,
				Collections.<String>singletonList(getName()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(oldValue));
	}

	@Override
	public V getAndRemove(K key) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(key == null){
			throw new NullPointerException();
		}
		
		DefaultRedisScript<String> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " +
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v;", 
				String.class);
		return configuration.getValueSerializer().deserialize(getJedisson().getRedisTemplate().execute(
				script,
				Collections.<String>singletonList(getName()),
				configuration.getKeySerializer().serialize(key)));
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
		return getJedisson().getRedisTemplate().execute(script,
				Collections.<String>singletonList(getName()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(oldValue),
				configuration.getValueSerializer().serialize(newValue));
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
		return getJedisson().getRedisTemplate().execute(script,
				Collections.<String>singletonList(getName()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value));
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
		return configuration.getValueSerializer().deserialize(getJedisson().getRedisTemplate().execute(script,
				Collections.<String>singletonList(getName()),
				configuration.getKeySerializer().serialize(key),
				configuration.getValueSerializer().serialize(value)));
	}

	@Override
	public void removeAll(Set<? extends K> keys) {
		if(isClosed()){
			throw new IllegalStateException("Cache:" + getName() + " is closed.");
		}
		if(keys == null || keys.contains(null)){
			throw new NullPointerException();
		}
		
		List<String> keyList = new LinkedList<>();
		for(K key : keys){
			keyList.add(configuration.getKeySerializer().serialize(key));
		}
		getJedisson().getRedisTemplate().opsForHash().delete(getName(), keyList.toArray());		
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
		
		getJedisson().getRedisTemplate().delete(getName());
	}

	public void setConfiguration(JedissonCacheConfiguration<K, V> configuration) {
		this.configuration = configuration;
	}

	public JedissonCacheConfiguration<K,V> getConfiguration(){
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
		
		return new EntryIterator(getJedisson().getRedisTemplate().<String,String>opsForHash().scan(getName(), ScanOptions.scanOptions().build()));
	}
	
	class EntryIterator implements Iterator<Cache.Entry<K, V>>{
		private Cursor<Map.Entry<String,String>> cursor;
		
		private Cache.Entry<K,V> curr;
		
		public EntryIterator(final Cursor<Map.Entry<String,String>> cursor){
			this.cursor = cursor;
		}

		@Override
		public boolean hasNext() {
			return cursor.hasNext();
		}

		@Override
		public Cache.Entry<K, V> next() {
			Map.Entry<String,String> entry = cursor.next(); 
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
