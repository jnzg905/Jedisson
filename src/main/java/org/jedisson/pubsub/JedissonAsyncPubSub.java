package org.jedisson.pubsub;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonAsyncPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.async.JedissonCommand.PUBLISH;

public class JedissonAsyncPubSub extends AbstractJedissonPubSub implements IJedissonAsyncPubSub{
	
	private static Map<String, JedissonAsyncPubSub> pubSubMap = new ConcurrentHashMap<>();
	
	public JedissonAsyncPubSub(String name, IJedissonSerializer serializer,Jedisson jedisson) {
		super(name, serializer, jedisson);
	}

	public static JedissonAsyncPubSub getPubSub(final String name, IJedissonSerializer serializer, Jedisson jedisson){
		JedissonAsyncPubSub pubsub = pubSubMap.get(name);
		if(pubsub == null){
			synchronized(pubSubMap){
				pubsub = pubSubMap.get(name);
				if(pubsub == null){
					pubsub = new JedissonAsyncPubSub(name,serializer,jedisson);
					pubSubMap.put(name, pubsub);
				}
			}
		}
		return pubsub;
	}
	
	@Override
	public <T> CompletableFuture<Long> publish(String channelName, T message) {
		PUBLISH command = new PUBLISH(channelName.getBytes(),getSerializer().serialize(message));
		return getJedisson().getAsyncService().execCommand(command);
	}
}
