package org.jedisson.pubsub;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonFuture;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.async.JedissonFuture;
import org.jedisson.async.JedissonCommand.PUBLISH;

public class JedissonAsyncPubSub extends JedissonPubSub{

	private static final ThreadLocal<IJedissonFuture> currFuture = new ThreadLocal<>();
	
	public JedissonAsyncPubSub(String name, IJedissonSerializer serializer,
			Jedisson jedisson) {
		super(name, serializer, jedisson);
		// TODO Auto-generated constructor stub
	}

	@Override
	public <T> void publish(String channelName, T message) {
		IJedissonFuture<T> future = new JedissonFuture(getSerializer());
		try{
			PUBLISH command = new PUBLISH(future,channelName.getBytes(),getSerializer().serialize(message));
			getJedisson().getAsyncService().sendCommand(command);	
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		currFuture.set(future);
	}

	@Override
	public IJedissonPubSub withAsync() {
		return this;
	}

	@Override
	public boolean isAsync() {
		return true;
	}

	@Override
	public <R> IJedissonFuture<R> future() {
		return currFuture.get();
	}

}
