package org.jedisson.autoconfiguration;

import java.net.UnknownHostException;

import org.jedisson.Jedisson;
import org.jedisson.RedisTemplateExecutor;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonRedisExecutor;
import org.jedisson.common.JedissonScriptExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
@EnableConfigurationProperties(JedissonConfiguration.class)
@ConditionalOnClass(IJedisson.class)
@ConditionalOnProperty(prefix="jedisson",value="enabled",matchIfMissing=true)
public class JedissonAutoConfiguration {

	@Autowired
    private JedissonConfiguration configuration;
	
	@Bean
	@ConditionalOnMissingBean(IJedissonRedisExecutor.class)
	public IJedissonRedisExecutor jedissonExecutor(RedisTemplate redisTemplate){
		IJedissonRedisExecutor executor = new RedisTemplateExecutor(redisTemplate);
		configuration.setExecutor(executor);
		return executor;
	}
	
//	@Bean
//	@ConditionalOnMissingBean(JedissonConfiguration.class)
//	public JedissonConfiguration jedissonConfiguration(IJedissonRedisExecutor redisExecutor){
//		configuration.setExecutor(redisExecutor);
//		return configuration;
//	}
//	
	@Bean
	@ConditionalOnMissingBean(IJedisson.class)
	public IJedisson jedisson(JedissonConfiguration jedissonConfiguration){
		return Jedisson.getJedisson(jedissonConfiguration);
	}
}
