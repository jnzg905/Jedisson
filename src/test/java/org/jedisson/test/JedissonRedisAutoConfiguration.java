package org.jedisson.test;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Cluster;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties.Sentinel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class JedissonRedisAutoConfiguration {
	
	@Autowired
	private RedisProperties properties;
	
	@Bean
	public RedisConnectionFactory jedisConnectionFactory(){ 
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		RedisProperties.Pool props = this.properties.getPool();
		poolConfig.setMaxTotal(props.getMaxActive());
		poolConfig.setMaxIdle(props.getMaxIdle());
		poolConfig.setMinIdle(props.getMinIdle());
		poolConfig.setMaxWaitMillis(props.getMaxWait());
		
		JedisConnectionFactory jedisConnectionFactory = null;
		Sentinel sentinelProperties = this.properties.getSentinel();
		if (sentinelProperties != null) {
			RedisSentinelConfiguration sentinelConfig = new RedisSentinelConfiguration();
			sentinelConfig.master(sentinelProperties.getMaster());
			sentinelConfig.setSentinels(createSentinels(sentinelProperties));
			jedisConnectionFactory = new JedisConnectionFactory(sentinelConfig,poolConfig);
		}else if(this.properties.getCluster() != null){
			Cluster clusterProperties = this.properties.getCluster();
			RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration(clusterProperties.getNodes());	
			if (clusterProperties.getMaxRedirects() != null) {
				clusterConfig.setMaxRedirects(clusterProperties.getMaxRedirects());
			}
			jedisConnectionFactory = new JedisConnectionFactory(clusterConfig,poolConfig);
		}else{
			jedisConnectionFactory = new JedisConnectionFactory(poolConfig);
		}
		 
		jedisConnectionFactory.setConvertPipelineAndTxResults(false);
		jedisConnectionFactory.setHostName(this.properties.getHost());
		jedisConnectionFactory.setPort(this.properties.getPort());
		if (this.properties.getPassword() != null) {
			jedisConnectionFactory.setPassword(this.properties.getPassword());
		}
		jedisConnectionFactory.setDatabase(this.properties.getDatabase());
		if (this.properties.getTimeout() > 0) {
			jedisConnectionFactory.setTimeout(this.properties.getTimeout());
		}
		return jedisConnectionFactory;

	}
	
	private List<RedisNode> createSentinels(Sentinel sentinel) {
		List<RedisNode> nodes = new ArrayList<RedisNode>();
		for (String node : StringUtils
				.commaDelimitedListToStringArray(sentinel.getNodes())) {
			try {
				String[] parts = StringUtils.split(node, ":");
				Assert.state(parts.length == 2, "Must be defined as 'host:port'");
				nodes.add(new RedisNode(parts[0], Integer.valueOf(parts[1])));
			}
			catch (RuntimeException ex) {
				throw new IllegalStateException(
						"Invalid redis sentinel " + "property '" + node + "'", ex);
			}
		}
		return nodes;
	}
}
