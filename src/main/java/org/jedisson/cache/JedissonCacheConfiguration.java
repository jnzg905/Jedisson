package org.jedisson.cache;

import java.util.HashSet;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.jedisson.api.IJedissonSerializer;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonStringSerializer;

public class JedissonCacheConfiguration<K,V> implements CompleteConfiguration<K,V>{

	protected String name;
	
	protected Class<K> keyType;
	
	protected Class<V> valueType;
	
	protected boolean isReadThrough;
	
	protected boolean isWriteThrough;

	protected boolean isStatisticsEnabled;

	protected boolean isStoreByValue;

	protected boolean isManagementEnabled;
	
	protected Factory<CacheLoader<K, V>> cacheLoaderFactory;
	
	protected Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory;

	protected Factory<ExpiryPolicy> expiryPolicyFactory;
	
	protected HashSet<CacheEntryListenerConfiguration<K,
    V>> listenerConfigurations;
	
	private IJedissonSerializer<K> keySerializer;
	
	private IJedissonSerializer<V> valueSerializer;
	
	public JedissonCacheConfiguration(){
		
	}
	
	public JedissonCacheConfiguration(final String name, final CompleteConfiguration<K,V> configuration){
		this.name = name;
		keyType = configuration.getKeyType();
		valueType = configuration.getValueType();
		isReadThrough = configuration.isReadThrough();
		isWriteThrough = configuration.isWriteThrough();
		isStatisticsEnabled = configuration.isStatisticsEnabled();
		isStoreByValue = configuration.isStoreByValue();
		isManagementEnabled = configuration.isManagementEnabled();
		cacheLoaderFactory = configuration.getCacheLoaderFactory();
		cacheWriterFactory = configuration.getCacheWriterFactory();
		expiryPolicyFactory = configuration.getExpiryPolicyFactory();
		listenerConfigurations = (HashSet<CacheEntryListenerConfiguration<K, V>>) configuration.getCacheEntryListenerConfigurations();
		
		if(keyType.equals(String.class)){
			keySerializer = (IJedissonSerializer<K>) new JedissonStringSerializer();
		}else{
			keySerializer = new JedissonFastJsonSerializer<K>(keyType);
		}
		
		valueSerializer = new JedissonFastJsonSerializer<V>(valueType);
	}
	
	public JedissonCacheConfiguration(final String name, final Configuration<K,V> configuration){
		this.name = name;
		keyType = configuration.getKeyType();
		valueType = configuration.getValueType();
		isStoreByValue = configuration.isStoreByValue();
		
		if(keyType.equals(String.class)){
			keySerializer = (IJedissonSerializer<K>) new JedissonStringSerializer();
		}else{
			keySerializer = new JedissonFastJsonSerializer<K>(keyType);
		}
		
		valueSerializer = new JedissonFastJsonSerializer<V>(valueType);
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public Class<K> getKeyType() {
		return keyType;
	}

	@Override
	public Class<V> getValueType() {
		return valueType;
	}

	@Override
	public boolean isStoreByValue() {
		return isStoreByValue;
	}

	@Override
	public boolean isReadThrough() {
		return isReadThrough;
	}

	@Override
	public boolean isWriteThrough() {
		return isWriteThrough;
	}

	@Override
	public boolean isStatisticsEnabled() {
		return isStatisticsEnabled;
	}

	@Override
	public boolean isManagementEnabled() {
		return isManagementEnabled;
	}

	@Override
	public Iterable<CacheEntryListenerConfiguration<K, V>> getCacheEntryListenerConfigurations() {
		return listenerConfigurations;
	}

	@Override
	public Factory<CacheLoader<K, V>> getCacheLoaderFactory() {
		return cacheLoaderFactory;
	}

	@Override
	public Factory<CacheWriter<? super K, ? super V>> getCacheWriterFactory() {
		return cacheWriterFactory;
	}

	@Override
	public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
		return expiryPolicyFactory;
	}

	public void setListenerConfigurations(
			HashSet<CacheEntryListenerConfiguration<K, V>> listenerConfigurations) {
		this.listenerConfigurations = listenerConfigurations;
	}

	public void setKeyType(Class<K> keyType) {
		this.keyType = keyType;
	}

	public void setValueType(Class<V> valueType) {
		this.valueType = valueType;
	}

	public void setReadThrough(boolean isReadThrough) {
		this.isReadThrough = isReadThrough;
	}

	public void setWriteThrough(boolean isWriteThrough) {
		this.isWriteThrough = isWriteThrough;
	}

	public void setStatisticsEnabled(boolean isStatisticsEnabled) {
		this.isStatisticsEnabled = isStatisticsEnabled;
	}

	public void setStoreByValue(boolean isStoreByValue) {
		this.isStoreByValue = isStoreByValue;
	}

	public void setManagementEnabled(boolean isManagementEnabled) {
		this.isManagementEnabled = isManagementEnabled;
	}

	public void setCacheLoaderFactory(Factory<CacheLoader<K, V>> cacheLoaderFactory) {
		this.cacheLoaderFactory = cacheLoaderFactory;
	}

	public void setCacheWriterFactory(
			Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory) {
		this.cacheWriterFactory = cacheWriterFactory;
	}

	public void setExpiryPolicyFactory(Factory<ExpiryPolicy> expiryPolicyFactory) {
		this.expiryPolicyFactory = expiryPolicyFactory;
	}

	public IJedissonSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(IJedissonSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public IJedissonSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	public void setValueSerializer(IJedissonSerializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

}
