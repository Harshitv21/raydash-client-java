package com.raydash.client.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/*
A plain Java bean (with only private fields & getters/setters) that Spring populates
automatically from configuration, thanks to @ConfigurationProperties(prefix = "raydash").
Spring finds every "raydash.<something>" entry whether it comes from application.properties
or an environment variable and matches it to a field here by name using "relaxed binding".
Meaning raydash-pool-size, raydash.poolSize & RAYDASH_POOL_SIZE (as an env var) all bind to
the same poolSize field below, without us having to write any parsing code at all! That
matching happens purely through reflection over these getters/setters. It's why they need
to follow the standard get/set Java Bean naming convention exactly or Spring won't find them.
*/
@ConfigurationProperties(prefix = "raydash")
public class RaydashProperties {
    private String host;
    private Integer port;
    private Integer defaultTTLSeconds = -1; // by default let's say a key never expires
    private Integer poolSize = 8;           // number of pooled TCP connections to the raydash server
    private String authToken;               // null/empty means no auth is configured from server
    
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public Integer getPort() { return port; }
    public void setPort(Integer port) { this.port = port; }
    
    public Integer getDefaultTTLSeconds() { return defaultTTLSeconds; }
    public void setDefaultTTLSeconds(Integer defaultTTLSeconds) { this.defaultTTLSeconds = defaultTTLSeconds; }

    public Integer getPoolSize() { return poolSize; }
    public void setPoolSize(Integer poolSize) { this.poolSize = poolSize; }

    public String getAuthToken() { return authToken; }
    public void setAuthToken(String authToken) { this.authToken = authToken; }
}
