package org.jedisson.common;

public class JedssionSerializationException extends RuntimeException{

	public JedssionSerializationException(String msg, Throwable throwable){
		super(msg,throwable);
	}
	
	public JedssionSerializationException(String msg){
		super(msg);
	}
}
