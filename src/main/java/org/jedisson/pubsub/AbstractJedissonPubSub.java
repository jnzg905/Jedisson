package org.jedisson.pubsub;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonMessageListener;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;

public abstract class AbstractJedissonPubSub extends JedissonObject {
	
	protected Map<String, Channel> channelMap = new ConcurrentHashMap<>();

	protected ExecutorService taskExecutor;

	protected SubscribeConnection[] subscribeConnections;

	protected ExecutorService connectionThreadPool;

	protected final IJedissonSerializer serializer;
	
	public AbstractJedissonPubSub(String name, IJedissonSerializer serializer, Jedisson jedisson) {
		super(name, jedisson);
		this.serializer = serializer;
		taskExecutor = Executors.newFixedThreadPool(10);
		subscribeConnections = new SubscribeConnection[2];
		connectionThreadPool = Executors.newFixedThreadPool(2);
	}
	
	public <T> void subscribe(String topic, IJedissonMessageListener<T> messageListener) {
		if(topic == null || topic.isEmpty()){
			throw new IllegalArgumentException("channelName must not null.");
		}
		if(messageListener == null){
			throw new IllegalArgumentException("listener must not null.");
		}
		synchronized(channelMap){
			Channel<T> channel  = channelMap.get(topic);
			if(channel == null){
				SubscribeConnection connection = getSubscribeConnection(topic);
				channel = new Channel<T>(topic,connection);	
				channel.subscribe();
				channelMap.put(topic, channel);
			}
			channel.addListener(messageListener);
		}
	}

	public void unsubscribe(String topic, IJedissonMessageListener listener) {
		if(topic == null || topic.isEmpty()){
			throw new IllegalArgumentException("channelName must not null.");
		}
		
		synchronized(channelMap){
			Channel channel  = channelMap.get(topic);
			if(channel != null){
				channel.unsubscribe(listener);
				if(!channel.isSubscribed()){
					channelMap.remove(topic);	
				}
			}	
		}
	}

	
	private SubscribeConnection getSubscribeConnection(final String channelName){
		int index = (channelName.hashCode() & Integer.MAX_VALUE) % subscribeConnections.length;
		if(subscribeConnections[index] == null){
			subscribeConnections[index] = new SubscribeConnection(index);
		}
		return subscribeConnections[index];
	}
	
	protected IJedissonSerializer getSerializer(){
		return serializer;
	}
	
	class SubscribeConnection{

		private final int id;
		
		private RedisConnection connection;
		
		public SubscribeConnection(int id){
			this.id = id;
		}
		
		public void subscribe(final String channelName){
			if(connection == null){
				connection = getJedisson().getExecutor().getConnectionFactory().getConnection();	
			}
			if(!connection.isSubscribed()){
				connectionThreadPool.submit(new Runnable(){

					@Override
					public void run() {
						try{
							connection.subscribe(new MessageListener(){

								@Override
								public void onMessage(Message message, byte[] pattern) {
									Channel channel = channelMap.get(new String(message.getChannel()));
									if(channel != null){
										channel.onMessage(message, pattern);
									}
								}
								
							}, channelName.getBytes());	
						}catch(Exception e){
							e.printStackTrace();
						}finally{
							connection.close();
						}
					}
					
				});
				while(!connection.isSubscribed()){
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}else{
				connection.getSubscription().subscribe(channelName.getBytes());
			}
		}
		
		public void unsubscribe(final String channelName){
			connection.getSubscription().unsubscribe(channelName.getBytes());	
		}
		
		public boolean isSubscribed(){
			return connection.isSubscribed();
		}
	}
	
	class Channel<T>{
		private final String channelName;
		
		private final SubscribeConnection connection;
		
		private List<IJedissonMessageListener<T>> listeners = new CopyOnWriteArrayList<>();
		
		public Channel(final String channelName, final SubscribeConnection connection){
			this.channelName = channelName;
			this.connection = connection;
		}

		public void subscribe(){
			connection.subscribe(channelName);
		}
		
		public void unsubscribe(IJedissonMessageListener listener){
			listeners.remove(listener);
			if(listeners.isEmpty()){
				connection.unsubscribe(channelName);	
			}
		}
		
		public boolean isSubscribed(){
			return connection.isSubscribed();
		}
		
		public String getChannelName() {
			return channelName;
		}
		
		public void addListener(IJedissonMessageListener<T> listener){
			listeners.add(listener);
		}

		public void onMessage(Message message, byte[] pattern) {
			for(final IJedissonMessageListener<T> listener : listeners){
				final T t = listener.getSerializer().deserialize(message.getBody());
				taskExecutor.submit(new Runnable(){

					@Override
					public void run() {
						try{
							listener.onMessage(t);	
						}catch(Exception e){
							e.printStackTrace();
						}
						
					}
					
				});
	
			}
						
		}

	}
}
