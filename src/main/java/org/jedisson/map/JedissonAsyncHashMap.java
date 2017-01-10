package org.jedisson.map;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncSupport;
import org.jedisson.api.IJedissonPromise;
import org.jedisson.api.IJedissonMap;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.async.JedissonCommand.DEL;
import org.jedisson.async.JedissonCommand.EVALSHA;
import org.jedisson.async.JedissonCommand.HEXISTS;
import org.jedisson.async.JedissonCommand.HGET;
import org.jedisson.async.JedissonCommand.HMSET;
import org.jedisson.async.JedissonCommand.HSET;
import org.jedisson.async.JedissonCommand.HVALS;
import org.jedisson.async.JedissonPromise;
import org.jedisson.async.JedissonCommand.LREM;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public class JedissonAsyncHashMap<K,V> extends JedissonHashMap<K,V>{

	private static final ThreadLocal<IJedissonPromise> currFuture = new ThreadLocal<>();
	
	public JedissonAsyncHashMap(String name,
			IJedissonSerializer<K> keySerializer,
			IJedissonSerializer<V> valueSerializer, Jedisson jedisson) {
		super(name, keySerializer, valueSerializer, jedisson);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean containsKey(Object key) {
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		HEXISTS command = new HEXISTS(future,getName().getBytes(),getKeySerializer().serialize((K) key));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean containsValue(Object value) {
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>(
				"local s = redis.call('hvals', KEYS[1]);" + 
				"for i = 1, #s, 1 do " + 
					"if ARGV[1] == s[i] then " + 
						"return 1 " + 
					"end " + 
				"end;" + 
				"return 0",Boolean.class);
		EVALSHA command = new EVALSHA(future,script, 1,getName().getBytes(),getValueSerializer().serialize((V) value));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public V get(Object key) {
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		HGET command = new HGET(future,getName().getBytes(),getKeySerializer().serialize((K) key));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public V put(K key, V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
				"redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); " + 
				"return v", 
				byte[].class);
		EVALSHA command = new EVALSHA(future,script, 1,
				getName().getBytes(),
				getKeySerializer().serialize(key),
				getValueSerializer().serialize(value));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public void fastPut(K key, V value) {
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		HSET command = new HSET(future,getName().getBytes(),getKeySerializer().serialize(key),
				getValueSerializer().serialize(value));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
	}

	@Override
	public V remove(Object key) {
		if (key == null) {
            throw new NullPointerException("map key can't be null");
        }
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('hget', KEYS[1], ARGV[1]); " + 
				"redis.call('hdel', KEYS[1], ARGV[1]); " + 
				"return v", 
				byte[].class);
		EVALSHA command = new EVALSHA(future,script, 1,
				getName().getBytes(),
				getKeySerializer().serialize((K) key));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(m.size());
		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			hashes.put(getKeySerializer().serialize(entry.getKey()), getValueSerializer().serialize(entry.getValue()));
		}
		HMSET command = new HMSET(future,getName().getBytes(),hashes);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
	}

	@Override
	public void clear() {
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		DEL command = new DEL(future,getName().getBytes());
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
	}

	@Override
	public Collection<V> values() {
		IJedissonPromise<V> future = new JedissonPromise<>(getValueSerializer());
		HVALS command = new HVALS(future,getName().getBytes());
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public IJedissonMap<K,V> withAsync() {
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
