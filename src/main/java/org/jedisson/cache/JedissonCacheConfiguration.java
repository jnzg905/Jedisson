package org.jedisson.cache;

import java.io.Serializable;

import javax.cache.configuration.Factory;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import org.jedisson.api.IJedissonSerializer;
import org.jedisson.serializer.JedissonFastJsonSerializer;
import org.jedisson.serializer.JedissonStringSerializer;

public class JedissonCacheConfiguration<K,V> implements Serializable {
	
	protected Class<K> keyType;
	
	protected Class<V> valueType;
	
	protected boolean isReadThrough = false;
	
	protected boolean isWriteThrough = false;

	protected boolean isStatisticsEnabled = false;

	protected boolean isStoreByValue = false;

	protected boolean isManagementEnabled = false;
	
	protected Factory<CacheLoader<K, V>> cacheLoaderFactory;
	
	protected Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory;

	protected Factory<ExpiryPolicy> expiryPolicyFactory;
	
	private IJedissonSerializer<K> keySerializer;
	
	private IJedissonSerializer<V> valueSerializer;
	
	public Class<K> getKeyType() {
		// TODO Auto-generated method stub
		return keyType;
	}

	public Class<V> getValueType() {
		// TODO Auto-generated method stub
		return valueType;
	}

	public boolean isReadThrough() {
		// TODO Auto-generated method stub
		return isReadThrough;
	}

	public boolean isWriteThrough() {
		// TODO Auto-generated method stub
		return isWriteThrough;
	}

	public boolean isStatisticsEnabled() {
		// TODO Auto-generated method stub
		return isStatisticsEnabled;
	}

	public boolean isManagementEnabled() {
		// TODO Auto-generated method stub
		return isManagementEnabled;
	}

	public Factory<CacheLoader<K, V>> getCacheLoaderFactory() {
		// TODO Auto-generated method stub
		return cacheLoaderFactory;
	}

	public Factory<CacheWriter<? super K, ? super V>> getCacheWriterFactory() {
		// TODO Auto-generated method stub
		return cacheWriterFactory;
	}

	public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
		// TODO Auto-generated method stub
		return expiryPolicyFactory;
	}

	public IJedissonSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public IJedissonSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	public boolean isStoreByValue() {
		return isStoreByValue;
	}

	public JedissonCacheConfiguration<K,V> setStoreByValue(boolean isStoreByValue) {
		this.isStoreByValue = isStoreByValue;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setKeyType(Class<K> keyType) {
		this.keyType = keyType;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setValueType(Class<V> valueType) {
		this.valueType = valueType;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setReadThrough(boolean isReadThrough) {
		this.isReadThrough = isReadThrough;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setWriteThrough(boolean isWriteThrough) {
		this.isWriteThrough = isWriteThrough;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setStatisticsEnabled(boolean isStatisticsEnabled) {
		this.isStatisticsEnabled = isStatisticsEnabled;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setManagementEnabled(boolean isManagementEnabled) {
		this.isManagementEnabled = isManagementEnabled;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setCacheLoaderFactory(Factory<CacheLoader<K, V>> cacheLoaderFactory) {
		this.cacheLoaderFactory = cacheLoaderFactory;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setCacheWriterFactory(
			Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory) {
		this.cacheWriterFactory = cacheWriterFactory;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setExpiryPolicyFactory(Factory<ExpiryPolicy> expiryPolicyFactory) {
		this.expiryPolicyFactory = expiryPolicyFactory;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setKeySerializer(IJedissonSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
		return this;
	}

	public JedissonCacheConfiguration<K,V> setValueSerializer(IJedissonSerializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
		return this;
	}
	
}
