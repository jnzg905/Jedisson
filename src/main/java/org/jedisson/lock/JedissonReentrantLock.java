package org.jedisson.lock;

import java.util.Arrays;
import java.util.Collections;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

public class JedissonReentrantLock extends JedissonLock{
	
	private long expiredTime = DEFAULT_LOCK_EXPIRATION_INTERVAL;
	
	public JedissonReentrantLock(String name, Jedisson jedisson) {
		super(name, jedisson);
	}
	
	@Override
	protected Long tryAcquire(long expiredTime) {
		this.expiredTime = expiredTime;
		RedisScript<Long> script = new DefaultRedisScript<>(
				 "if (redis.call('exists', KEYS[1]) == 0) then " +
					"redis.call('hset', KEYS[1], ARGV[2], 1); " +
	                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
	                "return nil; " +
	             "end; " +
	             "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
	                "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
	                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
	                "return nil; " +
	             "end; " +
	             "return redis.call('pttl', KEYS[1]);",
				Long.class);
		
		return (Long) getJedisson().getConfiguration().getExecutor().execute(
				script, 
				(IJedissonSerializer)null,
				Collections.<byte[]>singletonList(getName().getBytes()),
				serializer.serialize(expiredTime),
				serializer.serialize(getLockName(Thread.currentThread().getId())));
	}
	@Override
	protected boolean release() {
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				 "if (redis.call('exists', KEYS[1]) == 0) then " +
					"redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                 "end;" +
                 "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                 	"return 0;" +
                 "end; " +
                 "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                 "if (counter > 0) then " +
                 	"redis.call('pexpire', KEYS[1], ARGV[2]); " +
                 	"return 0; " +
                 "else " +
                 	"redis.call('del', KEYS[1]); " +
                 	"redis.call('publish', KEYS[2], ARGV[1]); " +
                 	"return 1; "+
                 "end; ",Boolean.class);
		
		return (boolean) getJedisson().getConfiguration().getExecutor().execute(
				script,
				(IJedissonSerializer)null,
				Arrays.<byte[]>asList(getName().getBytes(), getChannelName().getBytes()),
				serializer.serialize(UNLOCK_FLAG),
				serializer.serialize(expiredTime),
				serializer.serialize(getLockName(Thread.currentThread().getId())));
	}
	
	private String getLockName(long threadId){
		return uuid.toString() + ":" + threadId;
	}
}
