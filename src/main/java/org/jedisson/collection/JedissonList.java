package org.jedisson.collection;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.util.Assert;

public class JedissonList<E> extends AbstractJedissonCollection<E>{
	
	public JedissonList(final String name, Class<E> clss, IJedissonSerializer serializer, final Jedisson jedisson){
		super(name,clss,serializer,jedisson);
	}
	
	@Override
	public E get(final int index) {
		return (E) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<E>(){

			@Override
			public E doInRedis(RedisConnection connection)
					throws DataAccessException {
				return (E) getSerializer().deserialize(connection.lIndex(getName().getBytes(), index));
			}
			
		});
	}

	@Override
	public int size() {
		return (int) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Integer>(){

			@Override
			public Integer doInRedis(RedisConnection connection)
					throws DataAccessException {
				return connection.lLen(getName().getBytes()).intValue();
			}
			
		});
	}

	@Override
	public boolean add(final E e) {
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
	public boolean addAll(final Collection<? extends E> c) {
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		
		LinkedList<String> list = new LinkedList<>();
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[][] values = new byte[c.size()][];
				int i = 0;
				for(E v : c){
					values[i++] = getSerializer().serialize(v);
				}
				connection.rPush(getName().getBytes(), values);
				return true;
			}
			
		});
	}

	@Override
	public boolean addAll(final int index, final Collection<? extends E> c) {
		if(index < 0){
			throw new IndexOutOfBoundsException("index:" + index);
		}
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		
		if(index == 0){
			final List<E> elements = new ArrayList<>();
			elements.addAll(c);
			Collections.reverse(elements);
			return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

				@Override
				public Boolean doInRedis(RedisConnection connection)
						throws DataAccessException {
					byte[][] values = new byte[c.size()][];
					int i = 0;
					for(E v : elements){
						values[i++] = getSerializer().serialize(v);
					}
					connection.lPush(getName().getBytes(),values);
					return true;
				}
				
			});
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
				
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				params.toArray());
	}

	@Override
	public E set(int index, E element) {
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], ARGV[2]); " +
                "return v",byte[].class);
		return (E) getJedisson().getConfiguration().getExecutor().execute(script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getSerializer().serialize(index),
				getSerializer().serialize(element));
	}

	protected void fastSet(final int index, final E element){
		getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Object>(){

			@Override
			public Object doInRedis(RedisConnection connection)
					throws DataAccessException {
				connection.lSet(getName().getBytes(), index, getSerializer().serialize(element));
				return null;
			}
			
		});
	}
	
	@Override
	public void add(final int index, final E element) {
		addAll(index, Collections.singleton(element));
	}

	@Override
	public E remove(final int index) {
		if(index == 0){
			return (E) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<E>(){

				@Override
				public E doInRedis(RedisConnection connection)
						throws DataAccessException {
					return (E) getSerializer().deserialize(connection.lPop(getName().getBytes()));
				}
				
			});
		}
		RedisScript<byte[]> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_JEDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_JEDISSON');" +
                "return v",byte[].class);
		return (E) getJedisson().getConfiguration().getExecutor().execute(
				script, 
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getSerializer().serialize(index));
	}

	@Override
	public boolean remove(final Object o) {
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(new RedisCallback<Boolean>(){

			@Override
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				long ret = connection.lRem(getName().getBytes(), 1, getSerializer().serialize((E) o));
				return ret == 0 ? false : true;
			}
			
		});
	}

	@Override
	public int indexOf(Object o) {
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
		return ((Long)getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				getSerializer().serialize((E) o))).intValue();
	}

	@Override
	public int lastIndexOf(final Object o) {
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
		return ((Long)getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()),
				getSerializer().serialize((E) o))).intValue();
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
	public List<E> subList(int fromIndex, int toIndex) {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}
	
	@Override
	public boolean contains(Object o) {
		return indexOf(o) != -1;
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
				c.toArray());
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		if(c.isEmpty()){
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
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				getSerializer(),
				Collections.<byte[]>singletonList(getName().getBytes()), 
				params.toArray());
	}

	@Override
	public Iterator<E> iterator() {
		return listIterator(0);
	}

	@Override
	public ListIterator<E> listIterator() {
		// TODO Auto-generated method stub
		return listIterator(0);
	}

	@Override
	public ListIterator<E> listIterator(final int index) {

		return new ListIterator<E>(){
            private E prevCurrentValue;
            private E nextCurrentValue;
            private E currentValueHasRead;
            private int currentIndex = index - 1;
            private boolean hasBeenModified = true;

            @Override
            public boolean hasNext() {
                E val = JedissonList.this.get(currentIndex+1);
                if (val != null) {
                    nextCurrentValue = val;
                }
                return val != null;
            }

            @Override
            public E next() {
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
                JedissonList.this.remove(currentIndex);
                currentIndex--;
                hasBeenModified = true;
                currentValueHasRead = null;
            }

            @Override
            public boolean hasPrevious() {
                if (currentIndex < 0) {
                    return false;
                }
                E val = JedissonList.this.get(currentIndex);
                if (val != null) {
                    prevCurrentValue = val;
                }
                return val != null;
            }

            @Override
            public E previous() {
                if (prevCurrentValue == null && !hasPrevious()) {
                    throw new NoSuchElementException("No such element at index " + currentIndex);
                }
                currentIndex--;
                hasBeenModified = false;
                currentValueHasRead = prevCurrentValue;
                prevCurrentValue = null;
                return currentValueHasRead;
            }

            @Override
            public int nextIndex() {
                return currentIndex + 1;
            }

            @Override
            public int previousIndex() {
                return currentIndex;
            }

            @Override
            public void set(E e) {
                if (hasBeenModified) {
                    throw new IllegalStateException();
                }

                JedissonList.this.fastSet(currentIndex, e);
            }

            @Override
            public void add(E e) {
                JedissonList.this.add(currentIndex + 1, e);
                currentIndex++;
                hasBeenModified = true;
            }
		};
	}

	
}
