package org.jedisson.async;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jedisson.api.IJedissonSerializer;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.RedisScript;

public abstract class JedissonCommand<V> {
	protected byte[] key;
			
	protected long threadId;
	
	protected CompletableFuture<V> future;
	
	protected IJedissonSerializer<V> valueSerializer;
	
	public JedissonCommand(final byte[] key, IJedissonSerializer<V> valueSerializer){
		this.key = key;
		threadId = Thread.currentThread().getId();
		this.valueSerializer = valueSerializer;
	}

	public long getThreadId() {
		return threadId;
	}

	public CompletableFuture<V> getFuture(){
		return future;
	}
	
	public void setFuture(CompletableFuture<V> future){
		this.future = future;
	}
	
	public IJedissonSerializer<V> getValueSerializer(){
		return valueSerializer;
	}
	
	public abstract void execute(RedisConnection connection);
	
	public static class LINDEX<V> extends JedissonCommand<V>{

		private long index;
		
		public LINDEX(IJedissonSerializer<V> valueSerializer,byte[] key, long index) {
			super(key,valueSerializer);
			this.index = index;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lIndex(key, index);
		}
	}
	
	public static class LLEN extends JedissonCommand<Long>{

		public LLEN(byte[] key) {
			super(key,null);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lLen(key);
		}
		
	}
	
	public static class RPUSH extends JedissonCommand<Long>{
		private byte[][] values;
		
		public RPUSH(byte[] key, byte[]... values) {
			super(key,null);
			this.values = values;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.rPush(key, values);
		}
		
	}
	
	public static class LPUSH extends JedissonCommand<Long>{

		private byte[][] values;
		
		public LPUSH(byte[] key, byte[]... values) {
			super(key,null);
			this.values = values;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lPush(key, values);
		}
		
	}
	
	public static class LSET extends JedissonCommand<String>{
		
		private long index;
		private byte[] value;
		
		public LSET(byte[] key, long index, byte[] value) {
			super(key,null);
			this.index = index;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lSet(key, index, value);
		}
		
	}
	
	public static class LPOP<V> extends JedissonCommand<V>{
 
		public LPOP(IJedissonSerializer<V> serializer, byte[] key) {
			super(key,serializer);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lPop(key);
		}
		
	}
	
	public static class LREM extends JedissonCommand<Long>{
		private long count;
		private byte[] value;
		
		public LREM(byte[] key, long count, byte[] value) {
			super(key,null);
			this.count = count;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lRem(key, count, value);
		}
		
	}
	
	public static class DEL extends JedissonCommand<Long>{ 
		public DEL(byte[] key) {
			super(key,null);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.del(key);
		}
		
	}
	
	public static class LRANGE<V> extends JedissonCommand<V>{

		private long begin;
		private long end;
		
		public LRANGE(IJedissonSerializer<V> serializer, byte[] key, long begin, long end) {
			super(key,serializer);
			this.begin = begin;
			this.end = end;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.lRange(key, begin, end);
		}
		
	}
	
	public static class HLEN extends JedissonCommand<Long>{

		public HLEN(byte[] key) {
			super(key,null);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hLen(key);
		}
		
	}
	
	public static class HEXISTS extends JedissonCommand<Boolean>{

		private byte[] field;
		
		public HEXISTS(byte[] key, byte[] field) {
			super(key,null);
			this.field = field;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hExists(key, field);
		}
		
	}
	
	public static class HGET<V> extends JedissonCommand<V>{

		private byte[] field;
		
		public HGET(IJedissonSerializer<V> valueSerializer,byte[] key, byte[] field) {
			super(key, valueSerializer);
			this.field = field;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hGet(key, field);
		}
		
	}
	public static class HSET extends JedissonCommand<Long>{

		private byte[] field;
		private byte[] value;
		
		public HSET(byte[] key, byte[] field, byte[] value) {
			super(key,null);
			this.field = field;
			this.value = value;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hSet(key, field,value);
		}
		
	}
	
	public static class HMSET extends JedissonCommand<String>{
		private Map<byte[],byte[]> hashes;
		
		public HMSET(byte[] key, Map<byte[],byte[]> hashes) {
			super(key,null);
			this.hashes = hashes;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hMSet(key, hashes);
		}
		
	}
	
	public static class HMGET<V> extends JedissonCommand<V>{
		private byte[][] fields;
		
		public HMGET(IJedissonSerializer<V> valueSerializer,byte[] key,  byte[]... fields) {
			super(key,valueSerializer);
			this.fields = fields;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hMGet(key, fields);
		}
		
	}
	
	public static class HVALS<V> extends JedissonCommand<V>{

		public HVALS(IJedissonSerializer<V> serializer, byte[] key) {
			super(key,serializer);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hVals(key);
		}
		
	}
	
	public static class HDEL extends JedissonCommand<Long>{
		private byte[][] fields;
		
		public HDEL(byte[] key, byte[]... fields) {
			super(key,null);
			this.fields = fields;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hDel(key, fields);
		}
		
	}
	
	public static class HSCAN extends JedissonCommand{
		 ScanOptions scanOption;
		public HSCAN(IJedissonSerializer serializer, byte[] key, ScanOptions scanOption) {
			super(key,serializer);
			this.scanOption = scanOption;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.hScan(key, scanOption);
		}
		
	}
	
	public static class PUBLISH extends JedissonCommand<Long>{
		private byte[] message;
		
		public PUBLISH(byte[] key, byte[] message) {
			super(key,null);
			this.message = message;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.publish(key,message);
		}
		
	}
	
	public static class BLPOP<V> extends JedissonCommand<V>{
		private int timeout;
		
		public BLPOP(IJedissonSerializer<V> serializer, byte[] key, int timeout) {
			super(key,serializer);
			this.timeout = timeout;
		}

		@Override
		public void execute(RedisConnection connection) {
			connection.bLPop(timeout, key);
		}
		
	}
	
	public static class HSETNX extends JedissonCommand<Boolean>{

		private byte[] field;
		private byte[] value;
		
		public HSETNX(byte[] key, byte[] field, byte[] value) {
			super(key,null);
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
		
		public EVAL(RedisScript script,IJedissonSerializer valueSerializer, int keyNum, byte[]... keysAndArgs) {
			super(null,valueSerializer);
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
		public EVALSHA(RedisScript script, IJedissonSerializer valueSerializer, int keyNum, byte[]... keysAndArgs) {
			super(null,valueSerializer);
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
