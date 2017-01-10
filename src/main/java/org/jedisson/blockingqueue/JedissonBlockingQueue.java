package org.jedisson.blockingqueue;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncSupport;
import org.jedisson.api.IJedissonPromise;
import org.jedisson.api.IJedissonSerializer;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.Assert;

public class JedissonBlockingQueue<T> extends AbstractJedissonBlockingQueue<T>{

	public JedissonBlockingQueue(String name, IJedissonSerializer<T> serializer, Jedisson jedisson) {
		super(name, serializer,jedisson);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean add(final T e) {
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[] v = getSerializer().serialize(e);
				connection.rPush(getName().getBytes(), v);
				return true;
			}
			
		});
	}

	@Override
	public boolean offer(T e) {
		return add(e);
	}

	@Override
	public void put(T e) throws InterruptedException {
		add(e);
	}

	@Override
	public boolean offer(T e, long timeout, TimeUnit unit)
			throws InterruptedException {
		return offer(e);
	}

	@Override
	public T take() throws InterruptedException {
		return poll(0,TimeUnit.MILLISECONDS);
	}

	@Override
	public T poll(final long timeout, final TimeUnit unit) throws InterruptedException {
		return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T>(){

			@Override
			public T doInRedis(RedisConnection connection)
					throws DataAccessException {
				List<byte[]> results = connection.bLPop((int)unit.toSeconds(timeout), getName().getBytes());
				if(results != null){
					return (T) getSerializer().deserialize(results.get(1));
				}
				return null;
			}
			
		});
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean remove(final Object o) {
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				long ret = connection.lRem(getName().getBytes(), 1, getSerializer().serialize((T) o));
				return ret == 0 ? false : true;
			}
			
		});
	}

	@Override
	public boolean contains(Object o) {
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
		return getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getSerializer().serialize((T) o)) != -1;
	}

	@Override
	public int drainTo(Collection<? super T> c) {
		if (c == null) {
			throw new NullPointerException();
		}
	
		RedisScript<List> script = new DefaultRedisScript<>(
				"local vals = redis.call('lrange', KEYS[1], 0, -1); " +
				"redis.call('ltrim', KEYS[1], -1, 0); " +
				"return vals",List.class);
		
		List<T> results = getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()));
		
		c.addAll(results);
		return results.size();
	}

	@Override
	public int drainTo(Collection<? super T> c, int maxElements) {
		if (maxElements <= 0) {
			return 0;
		}
		
		if (c == null) {
			throw new NullPointerException();
		}
		RedisScript<List> script = new DefaultRedisScript<>(
				"local elemNum = math.min(ARGV[1], redis.call('llen', KEYS[1])) - 1;" +
				"local vals = redis.call('lrange', KEYS[1], 0, elemNum); " +
				"redis.call('ltrim', KEYS[1], elemNum + 1, -1); " +
				"return vals",List.class);
		
		List<T> results = getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				getSerializer().serialize(maxElements));
		
		c.addAll(results);
		return results.size();
	}

	@Override
	public T remove() {
		T value = poll();
		if (value == null) {
			throw new NoSuchElementException();
		}
		return value;
	}

	@Override
	public T poll() {
		return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T>(){

			@Override
			public T doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[] result = connection.lPop(getName().getBytes());
				return (T) (result == null ? null : getSerializer().deserialize(result));
			}
			
		});
	}

	@Override
	public T element() {
		return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T>(){

			@Override
			public T doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[] result = connection.lIndex(getName().getBytes(), 0L);
				if(result == null){
					throw new NoSuchElementException();
				}
				return (T) getSerializer().deserialize(result);
			}
			
		});
	}

	@Override
	public T peek() {
		return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T>(){

			@Override
			public T doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[] result = connection.lIndex(getName().getBytes(), 0L);
				return (T) (result == null ? null : getSerializer().deserialize(result));
			}
			
		});
	}

	@Override
	public int size() {
		return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Integer>(){

			@Override
			public Integer doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.lLen(getName().getBytes()).intValue();
			}
			
		});
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	private T get(final int index){
		return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T>(){

			@Override
			public T doInRedis(RedisConnection connection)
					throws DataAccessException {
				return (T) getSerializer().deserialize(connection.lIndex(getName().getBytes(), index));
			}
			
		});
	}
	
	private T remove(final int index) {
		if(index == 0){
			return getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T>(){

				@Override
				public T doInRedis(RedisConnection connection)
						throws DataAccessException {
					return (T) getSerializer().deserialize(connection.lPop(getName().getBytes()));
				}
				
			});
		}
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_JEDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_JEDISSON');" +
                "return v",byte[].class);
		return (T) getJedisson().getConfiguration().getExecutor().execute(
				script, 
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getSerializer().serialize(index));
	}
	
	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>(){
            private T nextCurrentValue;
            private T currentValueHasRead;
            private int currentIndex = -1;
            private boolean hasBeenModified = true;

            @Override
            public boolean hasNext() {
                T val = JedissonBlockingQueue.this.get(currentIndex + 1);
                if (val != null) {
                    nextCurrentValue = val;
                }
                return val != null;
            }

            @Override
            public T next() {
                if (nextCurrentValue == null && !hasNext()) {
                    throw new NoSuchElementException("No such element at index " + currentIndex);
                }
                currentIndex++;
                currentValueHasRead = nextCurrentValue;
                nextCurrentValue = null;
                hasBeenModified = false;
                return currentValueHasRead;
            }

            @Override
            public void remove() {
                if (currentValueHasRead == null) {
                    throw new IllegalStateException("Neither next nor previous have been called");
                }
                if (hasBeenModified) {
                    throw new IllegalStateException("Element been already deleted");
                }
                JedissonBlockingQueue.this.remove(currentIndex);
                currentIndex--;
                hasBeenModified = true;
                currentValueHasRead = null;
            }
		};
	}

	@Override
	public Object[] toArray() {
		return (Object[]) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object[]>(){

			@Override
			public Object[] doInRedis(RedisConnection connection)
					throws DataAccessException {
				List<byte[]> values = connection.lRange(getName().getBytes(), 0, -1);
				Object[] objs = new Object[values.size()];
				for(int i = 0; i < values.size(); i++){
					objs[i] = getSerializer().deserialize(values.get(i));
				}
				return objs;
			}
			
		});
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		return (T[]) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<T[]>(){

			@Override
			public T[] doInRedis(RedisConnection connection)
					throws DataAccessException {
				List<byte[]> values = connection.lRange(getName().getBytes(), 0, -1);
				int size = values.size();
				if(a.length < size){
					T[] copy = ((Object)a == (Object)Object[].class)
				            ? (T[]) new Object[size]
				            : (T[]) Array.newInstance(a.getClass().getComponentType(), size);
					for(int i = 0 ; i < copy.length; i++){
						copy[i] = (T) getSerializer().deserialize(values.get(i));
					}
					return (T[]) copy;
				}else{
					for(int i = 0 ; i < size; i++){
						a[i] = (T) getSerializer().deserialize(values.get(i));
					}
					a[size] = null;
					return a;
				}		
			}
			
		});
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		if(c.isEmpty()){
			return true;
		}
		List<byte[]> params = new LinkedList<>();
		for(Object v : c){
			params.add(getSerializer().serialize((T) v));
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
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				params.toArray());
	}

	@Override
	public boolean addAll(final Collection<? extends T> c) {
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[][] values = new byte[c.size()][];
				int i = 0;
				for(T v : c){
					values[i++] = getSerializer().serialize(v);
				}
				connection.rPush(getName().getBytes(), values);
				return true;
			}
			
		});
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		if(c.isEmpty()){
			return true;
		}
		
		List<byte[]> params = new LinkedList<>();
		for(Object v : c){
			params.add(getSerializer().serialize((T) v));
		}
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"local v = 0; " +
                "for i = 1, #ARGV, 1 do " + 
					"if redis.call('lrem', KEYS[1], 0, ARGV[i]) == 1 then " +
						"v = 1;" + 
					" end " +
                "end " + 
				"return v ",Boolean.class);
		
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				params.toArray());
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		if(c.isEmpty()){
			clear();
		}
		
		List<byte[]> params = new LinkedList<>();
		for(Object v : c){
			params.add(getSerializer().serialize((T) v));
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
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				params.toArray());
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
}
