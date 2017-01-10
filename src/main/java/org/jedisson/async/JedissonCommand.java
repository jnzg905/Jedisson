package org.jedisson.async;

import java.util.Map;

import org.jedisson.api.IJedissonPromise;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.RedisScript;

public abstract class JedissonCommand {
	protected byte[] key;
		
	protected IJedissonPromise future;
	
	protected long threadId;
	
	public JedissonCommand(IJedissonPromise future, final byte[] key){
		this.future = future;
		this.key = key;
		threadId = Thread.currentThread().getId();
	}

	public IJedissonPromise getFuture() {
		return future;
	}

	public void setFuture(IJedissonPromise future) {
		this.future = future;
	}

	public long getThreadId() {
		return threadId;
	}

	public abstract void execute(RedisConnection connection);
	
	public static class LINDEX extends JedissonCommand{

		private long index;
		
		public LINDEX(IJedissonPromise future, byte[] key, long index) {
			super(future, key);
			this.index = index;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lIndex(key, index);
		}
	}
	
	public static class LLEN extends JedissonCommand{

		public LLEN(IJedissonPromise future, byte[] key) {
			super(future, key);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lLen(key);
		}
		
	}
	
	public static class RPUSH extends JedissonCommand{
		private byte[][] values;
		
		public RPUSH(IJedissonPromise future, byte[] key, byte[]... values) {
			super(future, key);
			this.values = values;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.rPush(key, values);
		}
		
	}
	
	public static class LPUSH extends JedissonCommand{

		private byte[][] values;
		
		public LPUSH(IJedissonPromise future, byte[] key, byte[]... values) {
			super(future, key);
			this.values = values;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lPush(key, values);
		}
		
	}
	
	public static class LSET extends JedissonCommand{
		
		private long index;
		private byte[] value;
		
		public LSET(IJedissonPromise future, byte[] key, long index, byte[] value) {
			super(future, key);
			this.index = index;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lSet(key, index, value);
		}
		
	}
	
	public static class LPOP extends JedissonCommand{
 
		public LPOP(IJedissonPromise future, byte[] key) {
			super(future, key);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lPop(key);
		}
		
	}
	
	public static class LREM extends JedissonCommand{
		private long count;
		private byte[] value;
		
		public LREM(IJedissonPromise future, byte[] key, long count, byte[] value) {
			super(future, key);
			this.count = count;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lRem(key, count, value);
		}
		
	}
	
	public static class DEL extends JedissonCommand{ 
		public DEL(IJedissonPromise future, byte[] key) {
			super(future, key);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.del(key);
		}
		
	}
	
	public static class LRANGE extends JedissonCommand{

		private long begin;
		private long end;
		
		public LRANGE(IJedissonPromise future, byte[] key, long begin, long end) {
			super(future, key);
			this.begin = begin;
			this.end = end;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lRange(key, begin, end);
		}
		
	}
	
	public static class HLEN extends JedissonCommand{

		public HLEN(IJedissonPromise future, byte[] key) {
			super(future, key);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hLen(key);
		}
		
	}
	
	public static class HEXISTS extends JedissonCommand{

		private byte[] field;
		
		public HEXISTS(IJedissonPromise future, byte[] key, byte[] field) {
			super(future, key);
			this.field = field;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hExists(key, field);
		}
		
	}
	
	public static class HGET extends JedissonCommand{

		private byte[] field;
		
		public HGET(IJedissonPromise future, byte[] key, byte[] field) {
			super(future, key);
			this.field = field;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hGet(key, field);
		}
		
	}
	public static class HSET extends JedissonCommand{

		private byte[] field;
		private byte[] value;
		
		public HSET(IJedissonPromise future, byte[] key, byte[] field, byte[] value) {
			super(future, key);
			this.field = field;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hSet(key, field,value);
		}
		
	}
	
	public static class HMSET extends JedissonCommand{
		private Map<byte[],byte[]> hashes;
		
		public HMSET(IJedissonPromise future, byte[] key, Map<byte[],byte[]> hashes) {
			super(future, key);
			this.hashes = hashes;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hMSet(key, hashes);
		}
		
	}
	
	public static class HMGET extends JedissonCommand{
		private byte[][] fields;
		
		public HMGET(IJedissonPromise future, byte[] key, byte[]... fields) {
			super(future, key);
			this.fields = fields;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hMGet(key, fields);
		}
		
	}
	
	public static class HVALS extends JedissonCommand{

		public HVALS(IJedissonPromise future, byte[] key) {
			super(future, key);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hVals(key);
		}
		
	}
	
	public static class HDEL extends JedissonCommand{
		private byte[][] fields;
		
		public HDEL(IJedissonPromise future, byte[] key, byte[]... fields) {
			super(future, key);
			this.fields = fields;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hDel(key, fields);
		}
		
	}
	
	public static class HSCAN extends JedissonCommand{
		 ScanOptions scanOption;
		public HSCAN(IJedissonPromise future, byte[] key, ScanOptions scanOption) {
			super(future, key);
			this.scanOption = scanOption;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hScan(key, scanOption);
		}
		
	}
	
	public static class PUBLISH extends JedissonCommand{
		private byte[] message;
		
		public PUBLISH(IJedissonPromise future, byte[] key, byte[] message) {
			super(future, key);
			this.message = message;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.publish(key,message);
		}
		
	}
	
	public static class BLPOP extends JedissonCommand{
		private int timeout;
		
		public BLPOP(IJedissonPromise future, byte[] key, int timeout) {
			super(future, key);
			this.timeout = timeout;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.bLPop(timeout, key);
		}
		
	}
	
	public static class HSETNX extends JedissonCommand{

		private byte[] field;
		private byte[] value;
		
		public HSETNX(IJedissonPromise future, byte[] key, byte[] field, byte[] value) {
			super(future, key);
			this.field = field;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hSetNX(key, field,value);
		}
		
	}
	
	public static class EVAL extends JedissonCommand{
		RedisScript script;
		private int keyNum;
		private byte[][] keysAndArgs;
		
		public EVAL(IJedissonPromise future, RedisScript script, int keyNum, byte[]... keysAndArgs) {
			super(future, null);
			this.script = script;
			this.keyNum = keyNum;
			this.keysAndArgs = keysAndArgs;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.eval(script.getScriptAsString().getBytes(), ReturnType.fromJavaType(script.getResultType()),keyNum,keysAndArgs);
		}
		
	}
	
	public static class EVALSHA extends JedissonCommand{
		RedisScript script;
		private int keyNum;
		private byte[][] keysAndArgs;
		public EVALSHA(IJedissonPromise future, RedisScript script, int keyNum, byte[]... keysAndArgs) {
			super(future, null);
			this.script = script;
			this.keyNum = keyNum;
			this.keysAndArgs = keysAndArgs;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.evalSha(script.getSha1(),ReturnType.fromJavaType(script.getResultType()),keyNum,keysAndArgs);
		}
		
	}
}
