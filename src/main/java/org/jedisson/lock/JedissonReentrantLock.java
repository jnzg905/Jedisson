package org.jedisson.lock;

import java.util.Arrays;
import java.util.Collections;

import org.jedisson.Jedisson;
import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

public class JedissonReentrantLock extends JedissonLock{
	
	public JedissonReentrantLock(String name, Jedisson jedisson) {
		super(name, jedisson);
	}
	
	@Override
	protected boolean tryAcquire() {
		RedisScript<Boolean> script = new DefaultRedisScript<>(
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
				Boolean.class);
		
		return (boolean) getJedisson().getRedisTemplate().execute(
				script, 
				(IJedissonSerializer)null,
				(IJedissonSerializer)null,
				Collections.<String>singletonList(getName()),
				serializer.serialize(uuid.toString()),
				serializer.serialize(600000));
	}
	@Override
	protected boolean release() {
		RedisScript<Boolean> script = new DefaultRedisScript<>(
				 "if (redis.call('exists', KEYS[1]) == 0) then " +
                         "redis.call('publish', KEYS[2], ARGV[1]); " +
                         "return 1; " +
                     "end;" +
                     "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                         "return nil;" +
                     "end; " +
                     "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                     "if (counter > 0) then " +
                         "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                         "return 0; " +
                     "else " +
                         "redis.call('del', KEYS[1]); " +
                         "redis.call('publish', KEYS[2], ARGV[1]); " +
                         "return 1; "+
                     "end; " +
                     "return nil;" + 
				"end;",Boolean.class);
		
		return (boolean) getJedisson().getRedisTemplate().execute(
				script,
				(IJedissonSerializer)null,
				(IJedissonSerializer)null,
				Arrays.<Object>asList(getName(), getChannelName()),
				serializer.serialize(uuid.toString()),
				serializer.serialize(UNLOCK_FLAG));
	}
	
	private String getLockName(long threadId){
		return uuid.toString() + ":" + threadId;
	}
}
