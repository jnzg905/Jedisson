package org.jedisson.async;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jedisson.JedissonConfiguration;
import org.jedisson.api.IJedisson;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

public class JedissonAsyncService {
	 
	private FlushPool flushPool;
	
	private ScheduledExecutorService flushTimer;
	
	private IJedisson jedisson;
	
	public JedissonAsyncService(final IJedisson jedisson){
		this.jedisson = jedisson;
		flushPool = new FlushPool(jedisson.getConfiguration().getFlushThreadNum());
		
		flushTimer = Executors.newSingleThreadScheduledExecutor();
		flushTimer.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				flushPool.flush();
			}
			
		}, 0, jedisson.getConfiguration().getFlushFreq(), TimeUnit.MILLISECONDS);
	}
	public <V> CompletableFuture<V> execCommand(JedissonCommand<V> command){
		CompletableFuture<V> f = new CompletableFuture<V>();
		command.setFuture(f);
		try{
			flushPool.put(command);	
		}catch(Exception e){
			f.completeExceptionally(e);
		}
		return f;
	}
	
	class FlushPool{
		private int capacity = 1;
		
		private List<FlushTask> flushThreads;
		
		public FlushPool(int capacity){
			if(capacity > 0){
				this.capacity = capacity;	
			}
			flushThreads = new ArrayList<FlushTask>(this.capacity);
			for(int i = 0; i < this.capacity; i++){
				FlushTask flushThread = new FlushTask("FlushThread-" + i);
				flushThreads.add(flushThread);
				flushThread.start();
			}
		}
		
		public void flush(){
			for(FlushTask flushThread : flushThreads){
				flushThread.flush();
			}
		}
		
		public void put(JedissonCommand command) throws InterruptedException{
			int hash = (int) ((command.getThreadId() & Long.MAX_VALUE) % capacity);
			flushThreads.get(hash).put(command);
		}
	}
	
	class FlushTask extends Thread{
		private LinkedBlockingQueue<JedissonCommand> taskQueue = new LinkedBlockingQueue<>();
	
		private final ReentrantLock flushLock = new ReentrantLock();

		private final Condition notFlush = flushLock.newCondition();
		
		private volatile boolean isFlush = false;
		
		public FlushTask(final String name){
			super(name);
		}
		
		public void flush(){
			flushLock.lock();
			try{
				isFlush = true;
				notFlush.signal();	
			}finally{
				flushLock.unlock();
			}
		}
		
		public void put(JedissonCommand command) throws InterruptedException{
			taskQueue.put(command);
			if(!isFlush && taskQueue.size() >= jedisson.getConfiguration().getFlushSize()){
				flushLock.lock();
				try{
					isFlush = true;
					notFlush.signal();	
				}finally{
					flushLock.unlock();
				}
			}
		}
				
		@Override
		public void run() {
			while(!Thread.currentThread().isInterrupted()){
				flushLock.lock();
				final List<JedissonCommand> commands = new LinkedList<>();
				try{
					while(!isFlush){
						notFlush.await();
					}
					taskQueue.drainTo(commands,jedisson.getConfiguration().getFlushSize());
					
					if(!commands.isEmpty()){
						long startTime = System.currentTimeMillis();
						
						List results = jedisson.getExecutor().executePipeline(new RedisCallback<Object>(){

							@Override
							public Object doInRedis(RedisConnection connection) throws DataAccessException {
								for(JedissonCommand command : commands){
									command.execute(connection);
								}
								return null;
							}
							
						}, null);	
						assert(results.size() == commands.size());
						int i = 0;
						for(JedissonCommand command : commands){
							Object result = results.get(i++);
							Object v = jedisson.getExecutor().deserializeMixedResults(result, command.getValueSerializer());
							command.getFuture().complete(v);
						}
					}
					
				}catch(Exception e){
					for(JedissonCommand command : commands){
						command.getFuture().completeExceptionally(e);
					}
				}
				finally{
					isFlush = false;
					flushLock.unlock();
				}
			}
		}
		
	}
}
