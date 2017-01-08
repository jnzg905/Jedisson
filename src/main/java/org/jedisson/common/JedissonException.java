package org.jedisson.common;

public class JedissonException extends RuntimeException{

	public JedissonException(){
		
	}
	
	public JedissonException(String msg){
		super(msg);
	}
	
	public JedissonException(Throwable throwable){
		super(throwable);
	}
	
	public JedissonException(String msg, Throwable throwable){
		super(msg,throwable);
	}
}
