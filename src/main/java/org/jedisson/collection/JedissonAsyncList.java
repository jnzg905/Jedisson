package org.jedisson.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.api.collection.IJedissonAsyncList;
import org.jedisson.async.JedissonCommand.DEL;
import org.jedisson.async.JedissonCommand.EVALSHA;
import org.jedisson.async.JedissonCommand.LPOP;
import org.jedisson.async.JedissonCommand.LPUSH;
import org.jedisson.async.JedissonCommand.LRANGE;
import org.jedisson.async.JedissonCommand.LREM;
import org.jedisson.async.JedissonCommand.RPUSH;
import org.jedisson.async.JedissonCommand.LINDEX;
import org.jedisson.async.JedissonCommand.LLEN;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.Assert;

public class JedissonAsyncList<E> extends AbstractJedissonCollection<E> implements IJedissonAsyncList<E>{
	
	public JedissonAsyncList(String name,IJedissonSerializer<?> serializer, Jedisson jedisson) {
		super(name,serializer,jedisson);
	}

	@Override
	public CompletableFuture<Long> add(E v) {
		RPUSH command = new RPUSH(getName().getBytes(),getSerializer().serialize(v));
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Long> remove(Object o) {
		LREM command = new LREM(getName().getBytes(),1, getSerializer().serialize(o));
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Boolean> containsAll(Collection<?> c) {
		if(c.isEmpty()){
			return CompletableFuture.completedFuture(true);
		}
		
		List<byte[]> params = new LinkedList<>();
		for(Object v : c){
			params.add(getSerializer().serialize(v));
		}
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"local items = redis.call('lrange', KEYS[1], 0, -1) " +
                "for i=1, #items do " +
                "for j = 1, #ARGV, 1 do " +
                    "if items[i] == ARGV[j] then " +
                        "table.remove(ARGV, j) " +
                    "end " +
                "end " +
            "end " +
            "return #ARGV == 0 and 1 or 0",Boolean.class);
		
		return CompletableFuture.supplyAsync(() ->{
			return getJedisson().getExecutor().execute(script, getSerializer(), 
					Collections.<byte[]>singletonList(getName().getBytes()), 
					params.toArray());
		});
	}

	@Override
	public CompletableFuture<Long> addAll(Collection<? extends E> c) {
		if(c.isEmpty()){
			return CompletableFuture.completedFuture(null);
		}
		
		byte[][] values = new byte[c.size()][];
		int i = 0;
		for(E v : c){
			values[i++] = getSerializer().serialize(v);
		}
		RPUSH command = new RPUSH(getName().getBytes(),values);
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Boolean> addAll(int index, Collection<? extends E> c) {
		if(index < 0){
			throw new IndexOutOfBoundsException("index:" + index);
		}
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		
		if(index == 0){
			final List<E> elements = new ArrayList<>();
			elements.addAll(c);
			Collections.reverse(elements);
			byte[][] values = new byte[c.size()][];
			int i = 0;
			for(E v : elements){
				values[i++] = getSerializer().serialize(v);
			}
			LPUSH command = new LPUSH(getName().getBytes(),values);
			return getJedisson().getAsyncService().execCommand(command).thenApply(v -> true);
		}
		
		List<byte[]> params = new ArrayList<>(c.size() + 1);
		params.add(getSerializer().serialize(index));
		for(E e : c){
			params.add(getSerializer().serialize(e));
		}
		DefaultRedisScript<Boolean> script = new DefaultRedisScript<>(
				"local index = table.remove(ARGV, 1); " + // index is the first parameter
                "local size = redis.call('llen', KEYS[1]); " +
                "assert(tonumber(index) <= size, 'index: ' .. index .. ' but current size: ' .. size); " +
                "local tail = redis.call('lrange', KEYS[1], index, -1); " +
                "redis.call('ltrim', KEYS[1], 0, index - 1); " +
                "for i=1, #ARGV, 5000 do "
                    + "redis.call('rpush', KEYS[1], unpack(ARGV, i, math.min(i+4999, #ARGV))); "
                + "end " +
                "if #tail > 0 then " +
                    "for i=1, #tail, 5000 do "
                        + "redis.call('rpush', KEYS[1], unpack(tail, i, math.min(i+4999, #tail))); "
                  + "end "
              + "end;" +
                "return 1;",Boolean.class);
		
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()),
					params.toArray());
		});
	}

	@Override
	public CompletableFuture<Boolean> removeAll(Collection<?> c) {
		if(c.isEmpty()){
			return CompletableFuture.completedFuture(null);
		}
		
		List<byte[]> params = new LinkedList<>();
		for(Object v : c){
			params.add(getSerializer().serialize(v));
		}
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"local v = 0; " +
                "for i = 1, #ARGV, 1 do " + 
					"if redis.call('lrem', KEYS[1], 0, ARGV[i]) == 1 then " +
						"v = 1;" + 
					" end " +
                "end " + 
				"return v ",Boolean.class);
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					params.toArray());
		});
	}

	@Override
	public CompletableFuture<Boolean> retainAll(Collection<?> c) {
		if(c.isEmpty()){
			clear();
			return CompletableFuture.completedFuture(null);
		}
		
		List<byte[]> params = new LinkedList<>();
		for(Object v : c){
			params.add(getSerializer().serialize(v));
		}
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"local changed = 0 " +
                "local items = redis.call('lrange', KEYS[1], 0, -1) "
                + "local i = 1 "
                + "while i <= #items do "
                     + "local element = items[i] "
                     + "local isInAgrs = false "
                     + "for j = 1, #ARGV, 1 do "
                         + "if ARGV[j] == element then "
                             + "isInAgrs = true "
                             + "break "
                         + "end "
                     + "end "
                     + "if isInAgrs == false then "
                         + "redis.call('LREM', KEYS[1], 0, element) "
                         + "changed = 1 "
                     + "end "
                     + "i = i + 1 "
                + "end "
                + "return changed ",Boolean.class);
		
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					params.toArray());
		});
	}

	@Override
	public CompletableFuture<Long> clear() {
		DEL command = new DEL(getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<E> get(int index) {
		LINDEX command = new LINDEX(getSerializer(),getName().getBytes(),index);
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<E> set(int index, E element) {
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], ARGV[2]); " +
                "return v",byte[].class);
	
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getSerializer().serialize(index),
					getSerializer().serialize(element));
		});
	}

	@Override
	public CompletableFuture<Boolean> add(int index, E element) {
		return addAll(index, Collections.singleton(element));
	}

	@Override
	public CompletableFuture<E> remove(int index) {
		if(index == 0){
			LPOP command = new LPOP(getSerializer(),getName().getBytes());
			return getJedisson().getAsyncService().execCommand(command);
		}
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_JEDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_JEDISSON');" +
                "return v",byte[].class);
		
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getSerializer().serialize(index));
		});
	}

	@Override
	public CompletableFuture<List<E>> toArray() {
		LRANGE command = new LRANGE(getSerializer(),getName().getBytes(),0,-1);
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public CompletableFuture<Long> size() {
		LLEN command = new LLEN(getName().getBytes());
		return getJedisson().getAsyncService().execCommand(command);
	}

	@Override
	public boolean isEmpty() {
		return size().join() == 0;
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o).join() != -1;
	}

	@Override
	public CompletableFuture<Long> indexOf(Object o) {
		RedisScript<Long> script = new DefaultRedisScript<>(
				"local key = KEYS[1]; " +
                "local obj = ARGV[1]; " +
                "local items = redis.call('lrange', key, 0, -1); " +
                "for i=1, #items do " +
                    "if items[i] == obj then " +
                        "return i - 1 " +
                    "end " +
                "end " +
                "return -1",Long.class);
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()), 
					getSerializer().serialize((E) o));
		});
	}

	@Override
	public CompletableFuture<Long> lastIndexOf(Object o) {
		RedisScript<Long> script = new DefaultRedisScript<>(
				"local key = KEYS[1]; " +
                "local obj = ARGV[1]; " +
                "local items = redis.call('lrange', key, 0, -1); " +
                "for i = #items, 1, -1 do " +
                    "if items[i] == obj then " +
                        "return i - 1 " +
                    "end " +
                "end " +
                "return -1",Long.class);
		return CompletableFuture.supplyAsync(() -> {
			return getJedisson().getExecutor().execute(script,
					getSerializer(),
					Collections.<byte[]>singletonList(getName().getBytes()),
					getSerializer().serialize((E) o));
		});
	}

	@Override
	public CompletableFuture<List<E>> subList(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}
}
