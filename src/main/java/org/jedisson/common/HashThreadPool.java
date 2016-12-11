package org.jedisson.common;

import java.util.concurrent.LinkedBlockingQueue;

public class HashThreadPool {
	private final HashWorker[] workers;
	
	private int threadNum = 10;
	
	private int usedBucketNum = 0;
	
	private int eventCapacity = -1;
		
	public HashThreadPool(){
		this(10,-1);
	}
	
	public HashThreadPool(int threadNum){
		this(threadNum,-1);
	}
	
	public HashThreadPool(int threadNum, int capacity){
		this.threadNum = threadNum;
		this.eventCapacity = capacity;
		workers = new HashWorker[threadNum];
	}
	
	public void shutdown(){
		for(HashWorker worker : workers){
			if(worker != null){
				worker.interrupt();	
			}
		}
	}
	
	public void submit(Runnable task) throws InterruptedException{
		final int hash = task.hashCode();
		HashWorker worker = null;
		if (usedBucketNum >= threadNum) {
			worker = workers[rehash(hash, 0)];
		} else {
			for (int i = 0; i < threadNum; i++) {
				int bucket = rehash(hash, i);
				if (workers[bucket] == null) {
					synchronized (workers) {
						if (workers[bucket] == null) {
							worker = new HashWorker(hash, eventCapacity);
							workers[bucket] = worker;
							usedBucketNum++;
							worker.start();
							break;
						}
					}
				} else if (workers[bucket].hashCode == hash) {
					worker = workers[bucket];
					break;
				}
			}
		}
		worker.submit(task);
	}
		
	private int rehash(int bucket, int d){
		return ((bucket + d) & Integer.MAX_VALUE) % threadNum;
	}
	
	class HashWorker extends Thread{
		public int hashCode;
				
		private LinkedBlockingQueue<Runnable> taskQueue;
	
		public HashWorker(final int hash, final int capacity){
			setName("HashWorker-" + hash);
			hashCode = hash;
			taskQueue = (capacity == -1) ? new LinkedBlockingQueue<Runnable>() : new LinkedBlockingQueue<Runnable>(capacity);
		}
		
		public void submit(Runnable task) throws InterruptedException{
			taskQueue.put(task);
		}
				
		@Override
		public void run() {
			while(true){
				try {
					Runnable task = taskQueue.take();
					task.run();
					
				}catch(Exception e) {
					
				}
			}
		}
	}
}
