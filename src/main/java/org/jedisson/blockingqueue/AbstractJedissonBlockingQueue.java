package org.jedisson.blockingqueue;

import java.util.concurrent.BlockingQueue;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonBlockingQueue;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;

public abstract class AbstractJedissonBlockingQueue<T> extends JedissonObject implements IJedissonBlockingQueue<T>{

	private IJedissonSerializer<?> serializer;
	
	public AbstractJedissonBlockingQueue(String name, IJedissonSerializer<T> serializer, Jedisson jedisson) {
		super(name, jedisson);
		this.serializer = serializer;
	}

	public IJedissonSerializer getSerializer() {
		return serializer;
	}
}
