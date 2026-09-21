package com.raydash.client.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;

import com.raydash.client.RaydashClient;
import com.raydash.client.cache.RaydashCacheManager;

/*
This is the class that makes Raydash "just work" the moment someone adds raydash-client as a
dependency, with 0 manual @Bean wiring on their end. This is the whole point of Spring boot
autoconfiguration.

@AutoConfiguration marks this a class Spring Boot should consider loading automatically at
startup. It only actually gets picked up because it's also listed in this module's,
META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
file. This file is Spring Boot 4+ mechanism for autoconfiguration discovery. Without an entry
in that file, this class would just sit here unused, no matter how it's annotated.

@EnableConfigurationProperties(RaydashProperties.class) is what activates property binding
for RaydashProperties. Without this it's @ConfigurationProperties alone wouldn't cause 
Spring to actually populate it from application.properties env vars.
*/
@AutoConfiguration
@EnableConfigurationProperties(RaydashProperties.class)
public class RaydashAutoConfiguration {
    /*
    @ConditionalOnMissingBean is what makes this a well-behaved autoconfiguration rather 
    than a rigid one. If the application using this library already defines it's own
    RaydashClient bean (for whatever reason - custom pool size, testing, whatever), 
    Spring skips this one entirely and uses theirs instead. This is the standard Spring
    Boot "autoconfigure by default, but step aside if  the user configured their own"
    pattern.
    destroyMethod = "close" is what ties this bean's lifecycle to RaydashClient.close()
    (see RaydashClient.java) Spring will call that method automatically when the application
    context shuts down, so every pooled connection gets closed cleanly instead of just 
    abandoned when the JVM exits.

    This method is declared "throws Exception" because client.connect() can throw - if
    Raydash geuinely can't be reached after all of connect()'s interal retries, bean creation
    fails, which fails the whole Spring Boot application startup. It's always better to 
    fail loudly at startup than to silently run with a broken cache client.
    */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public RaydashClient raydashClient(RaydashProperties properties) throws Exception {
        RaydashClient client = new RaydashClient(
            properties.getHost(), 
            properties.getPort(), 
            properties.getPoolSize(),
            properties.getAuthToken()
        );

        client.connect();
        return client;
    }

    /*
    Similarly, only created if the application hasn't already registered its own CacheManager
    bean then this is the object Spring's @Cacheable machinery actually asks for a named Cache
    instance from.
    */
    @Bean 
    @ConditionalOnMissingBean 
    public CacheManager cacheManager(RaydashClient raydashClient, RaydashProperties properties) {
        return new RaydashCacheManager(raydashClient, properties.getDefaultTTLSeconds());
    }
}
