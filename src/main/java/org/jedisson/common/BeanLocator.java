package org.jedisson.common;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public final class BeanLocator implements ApplicationContextAware{

	private static ApplicationContext context;
	
	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		context = applicationContext;
	}
	
	public static <T> T getBean(final String name){
		return (T) context.getBean(name);
	}
	
	public static <T> T getBean(final Class<T> type){
		return context.getBean(type);
	}

}
