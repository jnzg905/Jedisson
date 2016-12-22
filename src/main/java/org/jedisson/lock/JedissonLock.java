package org.jedisson.lock;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonLock;
import org.jedisson.api.IJedissonMessageListener;
import org.jedisson.api.IJedissonPubSub;
import org.jedisson.api.IJedissonSerializer;
import org.jedisson.common.JedissonObject;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonStringSerializer;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

public class JedissonLock extends JedissonObject implements IJedissonLock{

	protected final static Long UNLOCK_FLAG = 0L;
	
	public static final long DEFAULT_LOCK_EXPIRATION_INTERVAL = 30000;
 
	private final static String LOCK_PUBSUB_NAME = "JedissonLock_PubSub";
	
	protected static Map<String,InternalLockEntry> internalLocks = new ConcurrentHashMap<>();
	
	protected final UUID uuid;
	
	protected InternalLockEntry lockEntry;
		
	protected IJedissonSerializer serializer = new JedissonFastJsonSerializer();
	
	public JedissonLock(String name, Jedisson jedisson) {
		super("JedissonLock:" + name,jedisson);
		
		this.uuid = UUID.randomUUID();
		lockEntry = internalLocks.get(name);
		if(lockEntry == null){
			synchronized(internalLocks){
				lockEntry = internalLocks.get(name);
				if(lockEntry == null){
					lockEntry = new InternalLockEntry(name);
					internalLocks.put(name, lockEntry);		
				}
			}
		}
	}

	@Override
	public void lock(){
		try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		final ReentrantLock lock = lockEntry.getLock();
		lockEntry.subscribe();
		lock.lockInterruptibly();
		try {
			while(true){
				Long ttl = tryAcquire(DEFAULT_LOCK_EXPIRATION_INTERVAL);
				if(ttl == null){
					return;
				}
				if(ttl >= 0){
					lockEntry.getCondition().awaitNanos(TimeUnit.MILLISECONDS.toNanos(ttl));
				}else{
					lockEntry.getCondition().awaitUninterruptibly();
				}
			}
		} finally {
			lockEntry.unSubscribe();
			lock.unlock();
		}
		
	}

	@Override
	public boolean tryLock() {
		final ReentrantLock lock = lockEntry.getLock();
		if(lock.tryLock()){
			try {
				return tryAcquire(DEFAULT_LOCK_EXPIRATION_INTERVAL) == null;
			} finally {
				lock.unlock();
			}	
		}
		return false;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit)
			throws InterruptedException {
		boolean ret = false;
		long nanos = unit.toNanos(time);
		long mills = unit.toMillis(time);
		final ReentrantLock lock = lockEntry.getLock();
		lockEntry.subscribe();
		lock.lock();
		try {
			while(true){
				Long ttl = tryAcquire(mills);
				if(ttl == null){
					return true;
				}
				if(nanos > 0){
					nanos = lockEntry.getCondition().awaitNanos(nanos);	
				}else{
					return false;
				}
			}
		} finally {
			lockEntry.unSubscribe();
			lock.unlock();
		}			
	}

	@Override
	public void unlock() {
		final ReentrantLock lock = lockEntry.getLock();
		lock.lock();
		try {
			if(release()){
				lockEntry.getCondition().signal();	
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

	protected void signalUnLock(){
		final ReentrantLock lock = lockEntry.getLock();
		lock.lock();
		try {
			lockEntry.getCondition().signal();
		} finally {
			lock.unlock();
		}
	}
	
	protected Long tryAcquire(long expiredTime){
		RedisScript<Long> script = new DefaultRedisScript<>(
				"if (redis.call('setnx', KEYS[1], ARGV[1]) == 1) then " +
					"redis.call('pexpire', KEYS[1], ARGV[2]); " + 
 					"return nil; " + 
				"else " + 
					"return redis.call('pttl',KEYS[1]); " + 
				"end;",
				Long.class);
		
		return (Long) getJedisson().getConfiguration().getExecutor().execute(
				script, 
				(IJedissonSerializer)null,
				Collections.<byte[]>singletonList(getName().getBytes()),
				serializer.serialize(uuid.toString()),
				serializer.serialize(expiredTime));
	}
	
	protected boolean release(){
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
		
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				(IJedissonSerializer)null,
				Arrays.<byte[]>asList(getName().getBytes(), getChannelName().getBytes()),
				serializer.serialize(uuid.toString()),
				serializer.serialize(UNLOCK_FLAG));
	}
	
	class JedissonLockMessageListener implements IJedissonMessageListener<Long>{

		private IJedissonSerializer serializer = new JedissonFastJsonSerializer(Long.class);
		
		@Override
		public void onMessage(Long t) {
			if(t.equals(UNLOCK_FLAG)){
				signalUnLock();
			}
		}

		@Override
		public IJedissonSerializer<Long> getSerializer() {
			return serializer;
		}
		
	}
	
	class InternalLockEntry{
		private final String name;
		
		private final ReentrantLock lock;
		
		private final Condition condition;
		
		private IJedissonPubSub lockPubSub;
		
		private JedissonLockMessageListener lockListener;
		
		public InternalLockEntry(final String name){
			this.name = name;
			lock = new ReentrantLock();
			condition = lock.newCondition();
			lockListener = new JedissonLockMessageListener();
			lockPubSub = getJedisson().getPubSub(LOCK_PUBSUB_NAME, new JedissonStringSerializer());
			
		}
		
		public String getName() {
			return name;
		}

		public ReentrantLock getLock() {
			return lock;
		}

		public Condition getCondition() {
			return condition;
		}

		public void subscribe(){
			lockPubSub.subscribe(getChannelName(),lockListener);
		}
		
		public void unSubscribe(){
			lockPubSub.unsubscribe(getChannelName(), lockListener);
		}
	}
}
