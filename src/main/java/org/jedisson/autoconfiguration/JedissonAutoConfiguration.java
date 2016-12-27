package org.jedisson.autoconfiguration;

import java.net.UnknownHostException;

import org.jedisson.Jedisson;
import org.jedisson.JedissonConfiguration;
import org.jedisson.RedisTemplateExecutor;
import org.jedisson.api.IJedisson;
import org.jedisson.api.IJedissonConfiguration;
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
@EnableConfigurationProperties(JedissonProperties.class)
@ConditionalOnClass(IJedisson.class)
@ConditionalOnProperty(prefix="jedisson",value="enabled",matchIfMissing=true)
public class JedissonAutoConfiguration {

	@Autowired
    private JedissonProperties properties;
	
	@Bean
	@ConditionalOnMissingBean(name = "redisTemplate")
	public RedisTemplate<Object, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory)
					throws UnknownHostException {
		RedisTemplate<Object, Object> template = new RedisTemplate<Object, Object>();
		template.setConnectionFactory(redisConnectionFactory);
		template.setEnableDefaultSerializer(false);
		template.setScriptExecutor(new JedissonScriptExecutor(template));
		return template;
	}
	
	@Bean
	@ConditionalOnMissingBean(IJedissonConfiguration.class)
	public IJedissonConfiguration jedissonConfiguration(RedisTemplate redisTemplate){
		JedissonConfiguration configuration = new JedissonConfiguration();
		configuration.setExecutor(new RedisTemplateExecutor(redisTemplate));
		configuration.setCacheManagerType(properties.getCacheManagerType());
		configuration.setKeySerializerType(properties.getKeySerializerType());
		configuration.setValueSerializerType(properties.getValueSerializerType());
		return configuration;
	}
	
	@Bean
	@ConditionalOnMissingBean(IJedisson.class)
	public IJedisson jedisson(IJedissonConfiguration jedissonConfiguration){
		return Jedisson.getJedisson(jedissonConfiguration);
	}
}
