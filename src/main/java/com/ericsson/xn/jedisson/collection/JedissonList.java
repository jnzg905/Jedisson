package com.ericsson.xn.jedisson.collection;

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

import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedissonObject;

public class JedissonList<E> extends AbstractJedissonCollection<E>{
		
	public JedissonList(final String name, IJedissonSerializer<E> serializer, final Jedisson jedisson){
		super(name,serializer,jedisson);
	}
	
	@Override
	public E get(int index) {
		return (E) getSerializer().deserialize(getJedisson().getRedisTemplate().opsForList().index(getName(), index));
	}

	@Override
	public int size() {
		return getJedisson().getRedisTemplate().opsForList().size(getName()).intValue();
	}

	@Override
	public boolean add(E e) {
		getJedisson().getRedisTemplate().opsForList().rightPush(getName(), getSerializer().serialize(e));
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		LinkedList<String> list = new LinkedList<>();
		for(E e : c){
			list.add(getSerializer().serialize(e));
		}
		if(!list.isEmpty()){
			getJedisson().getRedisTemplate().opsForList().rightPushAll(getName(), list);	
		}
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		if(index < 0){
			throw new IndexOutOfBoundsException("index:" + index);
		}
		if(c.isEmpty()){
			return true;
		}
		
		
		if(index == 0){
			List<String> elements = new ArrayList<>();
			for(E e : c){
				elements.add(getSerializer().serialize(e));
			}
			Collections.reverse(elements);
			getJedisson().getRedisTemplate().opsForList().leftPushAll(getName(), elements);
			return true;
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
		
		List<String> elements = new ArrayList<>(c.size() + 1);
		elements.add(String.valueOf(index));
        for(E e : c){
        	elements.add(getSerializer().serialize(e));
        }
		return getJedisson().getRedisTemplate().execute(script,Collections.<String>singletonList(getName()),
				elements.toArray());
	}

	@Override
	public E set(int index, E element) {
		RedisScript<String> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], ARGV[2]); " +
                "return v",String.class);
		return getSerializer().deserialize(getJedisson().getRedisTemplate().execute(script, 
				Collections.<String>singletonList(getName()), String.valueOf(index),getSerializer().serialize(element)));
	}

	protected void fastSet(int index,E element){
		getJedisson().getRedisTemplate().opsForList().set(getName(), index, getSerializer().serialize(element));
	}
	
	@Override
	public void add(final int index, final E element) {
		addAll(index, Collections.singleton(element));
	}

	@Override
	public E remove(final int index) {
		if(index == 0){
			return getSerializer().deserialize(getJedisson().getRedisTemplate().opsForList().leftPop(getName()));
		}
		RedisScript<String> script = new DefaultRedisScript<>(
				"local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_JEDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_JEDISSON');" +
                "return v",String.class);
		return getSerializer().deserialize(getJedisson().getRedisTemplate().execute(
				script, Arrays.asList(new String[]{getName()}), String.valueOf(index)));
	}

	@Override
	public boolean remove(Object o) {
		getJedisson().getRedisTemplate().opsForList().remove(getName(), 1, getSerializer().serialize((E) o));
		return true;
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
		return ((Long) getJedisson().getRedisTemplate().execute(script, Arrays.asList(new String[]{getName()}), 
				getSerializer().serialize((E) o))).intValue();
	}

	@Override
	public int lastIndexOf(Object o) {
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
		return ((Long)getJedisson().getRedisTemplate().execute(script, Arrays.asList(new String[]{getName()}),
				getSerializer().serialize((E) o))).intValue();
	}

	
	@Override
	public void clear() {
		getJedisson().getRedisTemplate().delete(getName());
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
		List<String> list = getJedisson().getRedisTemplate().opsForList().range(getName(), 0, -1);
		Object[] objs = new Object[list.size()];
		for(int i = 0; i < objs.length; i++){
			objs[i] = getSerializer().deserialize(list.get(i));
		}
		return objs;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		List<String> list = getJedisson().getRedisTemplate().opsForList().range(getName(), 0, -1);
		int size = list.size();
		if(a.length < size){
			T[] copy = ((Object)a == (Object)Object[].class)
		            ? (T[]) new Object[size]
		            : (T[]) Array.newInstance(a.getClass().getComponentType(), size);
			for(int i = 0 ; i < copy.length; i++){
				copy[i] = (T) getSerializer().deserialize(list.get(i));
			}
			return (T[]) copy;
		}else{
			for(int i = 0 ; i < size; i++){
				a[i] = (T) getSerializer().deserialize(list.get(i));
			}
			a[size] = null;
			return a;
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		if(c.isEmpty()){
			return true;
		}
		
		List<String> elements = new LinkedList<>();
		for(Object e : c){
			elements.add(getSerializer().serialize((E) e));
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
		return getJedisson().getRedisTemplate().execute(script, Collections.<String>singletonList(getName()), elements);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		if(c.isEmpty()){
			return true;
		}
		
		List<String> elements = new LinkedList<>();
		for(Object e : c){
			elements.add(getSerializer().serialize((E) e));
		}
		
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"local v = 0 " +
                "for i = 1, #ARGV, 1 do "
                + "if redis.call('lrem', KEYS[1], 0, ARGV[i]) == 1 "
                + "then v = 1 end "
            +"end "
           + "return v ",Boolean.class);
		
		return getJedisson().getRedisTemplate().execute(script, 
				Collections.<String>singletonList(getName()), elements.toArray());
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		if(c.isEmpty()){
			clear();
		}
		
		List<String> elements = new LinkedList<>();
		for(Object e : c){
			elements.add(getSerializer().serialize((E) e));
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
		return getJedisson().getRedisTemplate().execute(script, Collections.singletonList(getName()), elements.toArray());
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
