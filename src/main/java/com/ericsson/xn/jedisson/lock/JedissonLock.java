package com.ericsson.xn.jedisson.lock;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import com.ericsson.xn.jedisson.Jedisson;
import com.ericsson.xn.jedisson.api.IJedissonLock;
import com.ericsson.xn.jedisson.api.IJedissonMessageListener;
import com.ericsson.xn.jedisson.api.IJedissonPubSub;
import com.ericsson.xn.jedisson.api.IJedissonSerializer;
import com.ericsson.xn.jedisson.common.JedissonObject;
import com.ericsson.xn.jedisson.serializer.JedissonStringSerializer;

public class JedissonLock extends JedissonObject implements IJedissonLock{

	private final static String UNLOCK_FLAG = "0";
	
	private final static String LOCK_PUBSUB_NAME = "JedissonLock_PubSub";
	
	private static Map<String,JedissonLock> lockMap = new ConcurrentHashMap<>();
	
	private final ReentrantLock lock;
	
	private final Condition notLocked;
	
	private final UUID uuid;
	
	private IJedissonPubSub lockPubSub;
	
	private JedissonLockMessageListener lockListener;
	
	public JedissonLock(String name, Jedisson jedisson) {
		super(name,jedisson);
		this.uuid = UUID.randomUUID();
		this.lock = new ReentrantLock();
		this.notLocked = lock.newCondition();
		lockListener = new JedissonLockMessageListener();
		lockPubSub = jedisson.getPubSub(LOCK_PUBSUB_NAME, new JedissonStringSerializer());
		lockPubSub.subscribe(getChannelName(), lockListener);
	}

	public static JedissonLock getLock(final String name,Jedisson jedisson){
		JedissonLock lock = lockMap.get(name);
		if(lock == null){
			synchronized(lockMap){
				lock = lockMap.get(name);
				if(lock == null){
					lock = new JedissonLock(name,jedisson);
					lockMap.put(name, lock);
				}
			}
		}
		return lock;
	}
	
	@Override
	public void lock(){
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			while(!tryAcquire()){
				notLocked.awaitUninterruptibly();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		try {
			while(!tryAcquire()){
				notLocked.await();
			}
		} finally {
			lock.unlock();
		}
		
	}

	@Override
	public boolean tryLock() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return tryAcquire();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException {
		boolean ret = false;
		long nanos = unit.toNanos(time);
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			while(!(ret = tryAcquire()) && nanos > 0){
				nanos = notLocked.awaitNanos(nanos);
			}
			return ret;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void unlock() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if(release()){
				notLocked.signal();	
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Condition newCondition() {
		// TODO Auto-generated method stub
		return null;
	}

	protected String getChannelName(){
		if (getName().contains("{")) {
            return "jedisson_lock__channel:" + getName();
        }
        return "jedisson_lock__channel__{" + getName() + "}";
	}

	private void signalUnLock(){
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			notLocked.signal();
		} finally {
			lock.unlock();
		}
	}
	
	public boolean tryAcquire(){
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"if (redis.call('set', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) == nil) then " + 
					"return 0;" + 
				"else " + 
					"return 1;" + 
				"end;",Boolean.class);
		
		return getJedisson().getRedisTemplate().execute(script, Collections.<String>singletonList(getName()), 
				uuid.toString(),"30000");
	}
	
	public boolean release(){
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				"if (redis.call('get', KEYS[1]) == ARGV[1]) then " + 
					"if (redis.call('del', KEYS[1]) == 1) then " + 
						"redis.call('publish', KEYS[2], ARGV[2]);" +
						"return 1;" +
					"else " + 
						"return 0;" +
					"end;" + 
				"else " + 
					"redis.call('publish', KEYS[2], ARGV[2]);" +
					"return 1;" + 
				"end;",Boolean.class);
		
		return getJedisson().getRedisTemplate().execute(script, Arrays.<String>asList(getName(), getChannelName()), 
				uuid.toString(),UNLOCK_FLAG);
	}
	
	class JedissonLockMessageListener implements IJedissonMessageListener<String>{

		private IJedissonSerializer<String> serializer = new JedissonStringSerializer();
		
		@Override
		public void onMessage(String t) {
			if(t.equals(UNLOCK_FLAG)){
				signalUnLock();
			}
		}

		@Override
		public IJedissonSerializer<String> getSerializer() {
			return serializer;
		}
		
	}
}
