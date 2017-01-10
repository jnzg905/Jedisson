package org.jedisson.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonPromise;
import org.jedisson.api.IJedissonList;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.async.JedissonCommand.DEL;
import org.jedisson.async.JedissonCommand.EVALSHA;
import org.jedisson.async.JedissonCommand.LPOP;
import org.jedisson.async.JedissonCommand.LPUSH;
import org.jedisson.async.JedissonCommand.LRANGE;
import org.jedisson.async.JedissonCommand.LREM;
import org.jedisson.async.JedissonCommand.RPUSH;
import org.jedisson.async.JedissonPromise;
import org.jedisson.async.JedissonCommand.LINDEX;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.Assert;

public class JedissonAsyncList<E> extends JedissonList<E>{

	private static final ThreadLocal<IJedissonPromise> currFuture = new ThreadLocal<>();
	
	public JedissonAsyncList(String name, Class<E> clss,IJedissonSerializer serializer, Jedisson jedisson) {
		super(name, clss, serializer, jedisson);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean add(E v) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		RPUSH command = new RPUSH(future,getName().getBytes(),getSerializer().serialize(v));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean remove(Object o) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		LREM command = new LREM(future,getName().getBytes(),1, getSerializer().serialize(o));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		if(c.isEmpty()){
			future.setSuccess(null);
			currFuture.set(future);
			return true;
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
		
		final byte[][] keysAndArgs = new byte[c.size() + 1][];
		int j = 0;
		keysAndArgs[j++] = getName().getBytes();
		for(Object v : c){
			keysAndArgs[j++] = getSerializer().serialize(v);
		}
		EVALSHA command = new EVALSHA(future,script, 1,keysAndArgs);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		if(c.isEmpty()){
			future.setSuccess(null);
			currFuture.set(future);
			return true;
		}
		
		byte[][] values = new byte[c.size()][];
		int i = 0;
		for(E v : c){
			values[i++] = getSerializer().serialize(v);
		}
		RPUSH command = new RPUSH(future,getName().getBytes(),values);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		if(index < 0){
			throw new IndexOutOfBoundsException("index:" + index);
		}
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		
		if(index == 0){
			final List<E> elements = new ArrayList<>();
			elements.addAll(c);
			Collections.reverse(elements);
			IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
			byte[][] values = new byte[c.size()][];
			int i = 0;
			for(E v : elements){
				values[i++] = getSerializer().serialize(v);
			}
			LPUSH command = new LPUSH(future,getName().getBytes(),values);
			try {
				getJedisson().getAsyncService().sendCommand(command);
			} catch (InterruptedException e) {
				future.setFailure(e);
			}
			currFuture.set(future);
			return true;
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
		
		final byte[][] keysAndArgs = new byte[c.size() + 2][];
		int j = 0;
		keysAndArgs[j++] = getName().getBytes();
		keysAndArgs[j++] = getSerializer().serialize(index);
		for(Object v : c){
			keysAndArgs[j++] = getSerializer().serialize(v);
		}
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		EVALSHA command = new EVALSHA(future,script, 1,keysAndArgs);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		if(c.isEmpty()){
			future.setSuccess(null);
			currFuture.set(future);
			return true;
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
		
		final byte[][] keysAndArgs = new byte[c.size() + 1][];
		int j = 0;
		keysAndArgs[j++] = getName().getBytes();
		for(Object v : c){
			keysAndArgs[j++] = getSerializer().serialize(v);
		}
		EVALSHA command = new EVALSHA(future,script,1,keysAndArgs);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		if(c.isEmpty()){
			clear();
			return true;
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

		final byte[][] keysAndArgs = new byte[c.size() + 1][];
		int j = 0;
		keysAndArgs[j++] = getName().getBytes();
		for(Object v : c){
			keysAndArgs[j++] = getSerializer().serialize(v);
		}
		
		EVALSHA command = new EVALSHA(future,script,1,keysAndArgs);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return true;
	}

	@Override
	public void clear() {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		DEL command = new DEL(future,getName().getBytes());
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
	}

	@Override
	public E get(int index) {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		LINDEX command = new LINDEX(future,getName().getBytes(),index);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public E set(int index, E element) {
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], ARGV[2]); " +
                "return v",byte[].class);
	
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		EVALSHA command = new EVALSHA(future,script, 1,getName().getBytes(),getSerializer().serialize(index),
				getSerializer().serialize(element));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public void add(int index, E element) {
		addAll(index, Collections.singleton(element));
	}

	@Override
	public E remove(int index) {
		if(index == 0){
			IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
			LPOP command = new LPOP(future,getName().getBytes());
			try {
				getJedisson().getAsyncService().sendCommand(command);
			} catch (InterruptedException e) {
				future.setFailure(e);
			}
			currFuture.set(future);
		}
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_JEDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_JEDISSON');" +
                "return v",byte[].class);
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		EVALSHA command = new EVALSHA(future,script,1,getName().getBytes(),getSerializer().serialize(index));
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public Object[] toArray() {
		IJedissonPromise<E> future = new JedissonPromise<>(getSerializer());
		LRANGE command = new LRANGE(future,getName().getBytes(),0,-1);
		try {
			getJedisson().getAsyncService().sendCommand(command);
		} catch (InterruptedException e) {
			future.setFailure(e);
		}
		currFuture.set(future);
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return super.toArray(a);
	}

	@Override
	public IJedissonList<E> withAsync() {
		return this;
	}

	@Override
	public boolean isAsync() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public <R> IJedissonPromise<R> future() {
		return currFuture.get();
	}

}
