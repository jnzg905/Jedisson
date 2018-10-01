package org.jedisson.collection;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonList;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.api.collection.IJedissonAsyncList;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.util.Assert;

public class JedissonList<E> extends AbstractJedissonCollection<E> implements IJedissonList<E>{
	
	private IJedissonAsyncList<E> asyncList;
	
	public JedissonList(final String name, IJedissonSerializer<E> serializer, final Jedisson jedisson){
		super(name,serializer,jedisson);
		asyncList = jedisson.getAsyncList(name, serializer);
	}
	
	@Override
	public E get(final int index) {
		return asyncList.get(index).join();	
	}

	@Override
	public int size() {
		return asyncList.size().join().intValue();
	}

	@Override
	public boolean add(final E e) {
		asyncList.add(e).join();
		return true;
	}

	@Override
	public boolean addAll(final Collection<? extends E> c) {
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		asyncList.addAll(c).join();
		return true;
	}

	@Override
	public boolean addAll(final int index, final Collection<? extends E> c) {
		if(index < 0){
			throw new IndexOutOfBoundsException("index:" + index);
		}
		Assert.notEmpty(c, "Values must not be 'null' or empty.");
		
		return asyncList.addAll(index, c).join();
	}

	@Override
	public E set(int index, E element) {
		return asyncList.set(index, element).join();
	}

	protected void fastSet(final int index, final E element){
		getJedisson().getExecutor().execute(new RedisCallback<Object>(){

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
		return asyncList.remove(index).join();
	}

	@Override
	public boolean remove(final Object o) {
		return (asyncList.remove(o).join() == 0) ? false : true;
	}

	@Override
	public int indexOf(Object o) {
		return asyncList.indexOf(o).join().intValue();
	}

	@Override
	public int lastIndexOf(final Object o) {
		return asyncList.lastIndexOf(o).join().intValue();
	}

	
	@Override
	public void clear() {
		asyncList.clear().join();
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
		return asyncList.toArray().thenApply(values -> {
			return values.toArray();
		}).join();
	}

	@Override
	public <T> T[] toArray(final T[] a) {
		return asyncList.toArray().thenApply(values -> {
			int size = values.size();
			if(a.length < size){
				T[] copy = ((Object)a == (Object)Object[].class)
			            ? (T[]) new Object[size]
			            : (T[]) Array.newInstance(a.getClass().getComponentType(), size);
				for(int i = 0 ; i < copy.length; i++){
					copy[i] = (T) values.get(i);
				}
				return copy;
			}else{
				for(int i = 0 ; i < size; i++){
					a[i] = (T) values.get(i);
				}
				a[size] = null;
				return a;
			}	
		}).join();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return asyncList.containsAll(c).join();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return asyncList.removeAll(c).join();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return asyncList.retainAll(c).join();
	}

	@Override
	public Iterator<E> iterator() {
		return listIterator(0);
	}

	@Override
	public ListIterator<E> listIterator() {
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
