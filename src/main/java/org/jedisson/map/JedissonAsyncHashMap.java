package org.jedisson.map;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.api.map.IJedissonAsyncMap;
import org.jedisson.async.JedissonCommand.DEL;
import org.jedisson.async.JedissonCommand.HDEL;
import org.jedisson.async.JedissonCommand.HEXISTS;
import org.jedisson.async.JedissonCommand.HGET;
import org.jedisson.async.JedissonCommand.HLEN;
import org.jedisson.async.JedissonCommand.HMSET;
import org.jedisson.async.JedissonCommand.HSET;
import org.jedisson.async.JedissonCommand.HVALS;
import org.springframework.data.redis.core.script.DefaultRedisScript;

public class JedissonAsyncHashMap<K, V> extends AbstractJedissonMap<K, V> implements IJedissonAsyncMap<K, V> {

	public JedissonAsyncHashMap(String name, IJedissonSerializer<K> keySerializer,
			IJedissonSerializer<V> valueSerializer, Jedisson jedisson) {
		super(name, keySerializer, valueSerializer, jedisson);
	}

	@Override
	public CompletableFuture<Boolean> containsKey(Object key) {
		HEXISTS command = new HEXISTS(getName().getBytes(),
				getKeySerializer().serialize((K) key));
		return getJedisson().getAsyncService().execCommand(command);

	}

	@Override
	public CompletableFuture<Boolean> containsValue(Object value) {
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}

		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>("local s = redis.call('hvals', KEYS[1]);"
				+ "for i = 1, #s, 1 do " + "if ARGV[1] == s[i] then " + "return 1 " + "end " + "end;" + "return 0",
				Boolean.class);
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getValueSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()),
					getValueSerializer().serialize((V) value));
		});
	}

	@Override
	public CompletableFuture<V> get(Object key) {
		HGET command = new HGET(getValueSerializer(), getName().getBytes(), getKeySerializer().serialize((K) key));
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<V> put(K key, V value) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		if (value == null) {
			throw new NullPointerException("map value can't be null");
		}
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>("local v = redis.call('hget', KEYS[1], ARGV[1]); "
				+ "redis.call('hset', KEYS[1], ARGV[1], ARGV[2]); " + "return v", byte[].class);
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getValueSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getKeySerializer().serialize(key),
					getValueSerializer().serialize(value));
		});
	}

	@Override
	public CompletableFuture<Long> fastPut(K key, V value) {
		HSET command = new HSET(getName().getBytes(), getKeySerializer().serialize(key),
				getValueSerializer().serialize(value));
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<V> remove(Object key) {
		if (key == null) {
			throw new NullPointerException("map key can't be null");
		}
		DefaultRedisScript<byte[]> script = new DefaultRedisScript<>("local v = redis.call('hget', KEYS[1], ARGV[1]); "
				+ "redis.call('hdel', KEYS[1], ARGV[1]); " + "return v", byte[].class);
		
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getValueSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getKeySerializer().serialize((K) key));
		});
	}

	@Override
	public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m) {
		final Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(m.size());
		for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
			hashes.put(getKeySerializer().serialize(entry.getKey()), getValueSerializer().serialize(entry.getValue()));
		}
		HMSET command = new HMSET(getName().getBytes(), hashes);
		return getJedisson().getAsyncService().execCommand(command).thenAccept(v -> {});
	}

	@Override
	public CompletableFuture<Long> clear() {
		DEL command = new DEL(getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Collection<V>> values() {
		HVALS command = new HVALS(getValueSerializer(), getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Long> size() {
		HLEN command = new HLEN(getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Boolean> isEmpty() {
		return size().thenApply(v -> {
			return v == 0;
		});
	}

	@Override
	public CompletableFuture<V> putIfAbsent(K key, V value) {
		return get(key).thenComposeAsync(v -> {
			if (v == null) {
				return put(key, value);
			}
			return CompletableFuture.completedFuture(v);
		});
	}

	@Override
	public CompletableFuture<Long> fastRemove(K... keys) {
		byte[][] hKeys = new byte[keys.length][];
		int i = 0;
		for(K key : keys){
			hKeys[i++] = getKeySerializer().serialize(key);
		}
		HDEL command = new HDEL(getName().getBytes(),hKeys);
		return getJedisson().getAsyncService().execCommand(command);
	}
}
