package org.jedisson.api;

public interface IJedissonAsyncSupport {

	public <T extends IJedissonAsyncSupport> T withAsync();
	
	public boolean isAsync();
	
	public <R> IJedissonPromise<R> future();
}
