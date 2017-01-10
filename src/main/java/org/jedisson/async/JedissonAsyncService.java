package org.jedisson.async;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonPromise;
import org.jedisson.autoconfiguration.JedissonConfiguration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;

public class JedissonAsyncService {
	 
	private FlushPool flushPool;
	
	private ScheduledExecutorService flushTimer;
	
	private IJedisson jedisson;
	
	public JedissonAsyncService(final IJedisson jedisson){
		this.jedisson = jedisson;
		flushPool = new FlushPool(jedisson.getConfiguration().getAsync().getThreadNum());
		
		flushTimer = Executors.newSingleThreadScheduledExecutor();
		flushTimer.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				flushPool.flush();
			}
			
		}, 0, jedisson.getConfiguration().getAsync().getFlushFreq(), TimeUnit.MILLISECONDS);
	}
	public void sendCommand(JedissonCommand command) throws InterruptedException{
		flushPool.put(command);
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
			if(!isFlush && taskQueue.size() >= jedisson.getConfiguration().getAsync().getFlushSize()){
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
					taskQueue.drainTo(commands,jedisson.getConfiguration().getAsync().getFlushSize());
					
					if(!commands.isEmpty()){
						long startTime = System.currentTimeMillis();
						
						List results = jedisson.getConfiguration().getExecutor().executePipeline(new RedisCallback<Object>(){

							@Override
							public Object doInRedis(RedisConnection connection)
									throws DataAccessException {
								for(JedissonCommand command : commands){
									command.execute(connection);
								}
								return null;
							}
							
						}, null);	
						System.out.println("flush:" + (System.currentTimeMillis() - startTime) + ",size=" + results.size());
						int i = 0;
						for(JedissonCommand command : commands){
							command.getFuture().setSuccess(results.get(i));
						}
						System.out.println("done:" + (System.currentTimeMillis() - startTime));
					}
					
				}catch(Exception e){
					for(JedissonCommand command : commands){
						command.getFuture().setFailure(e);
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
