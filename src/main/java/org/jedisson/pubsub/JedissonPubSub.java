package org.jedisson.pubsub;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncSupport;
import org.jedisson.api.IJedissonMessageListener;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

public class JedissonPubSub extends AbstractJedissonPubSub implements IJedissonPubSub, IJedissonAsyncSupport{

	private static Map<String, JedissonPubSub> pubSubMap = new ConcurrentHashMap<>();
	
	public JedissonPubSub(String name, IJedissonSerializer serializer, Jedisson jedisson) {
		super(name,serializer,jedisson);
	}

	public static JedissonPubSub getPubSub(final String name, IJedissonSerializer serializer, Jedisson jedisson){
		JedissonPubSub pubsub = pubSubMap.get(name);
		if(pubsub == null){
			synchronized(pubSubMap){
				pubsub = pubSubMap.get(name);
				if(pubsub == null){
					pubsub = new JedissonPubSub(name,serializer,jedisson);
					pubSubMap.put(name, pubsub);
				}
			}
		}
		return pubsub;
	}
	
	@Override
	public <T> void publish(final String channelName, final T message) {
		getJedisson().getExecutor().execute(new RedisCallback<T>(){

			@Override
			public T doInRedis(RedisConnection connection) throws DataAccessException {
				connection.publish(channelName.getBytes(), serializer.serialize(message));
				return null;
			}
		});		
	}
}
