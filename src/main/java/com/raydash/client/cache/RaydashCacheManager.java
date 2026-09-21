package com.raydash.client.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;

import com.raydash.client.RaydashClient;

/*
Spring's CacheManager is the thing @Cacheable actually asks for a named Cache from (e.g. "give
me the Cache called 'users'"). AbstractCacheManager is a base class that splits that job into 2
pieces we implement below:
1. loadCaches() for any caches that are known up front,
2. and getMissingCache() for ones that show up later, by name on demand.
*/
public class RaydashCacheManager extends AbstractCacheManager {
    private final RaydashClient client;
    private final Integer defaultTTLSeconds;

    /*
    ConcurrentHashMap not a plain HashMap, because multiple threads can call getMissingCache()
    for the same brand new cache name at roughly the same time (imagine 2 concurrent requests
    both hitting @Cacheable "orders" for the very first time). computeIfAbsent() below is what
    makes that race safe. It guarantees only one RaydashCache ever actually gets constructed
    and stored per name, even if several threads call it simultaneously. A plain old HashMap
    with a manual "check then insert" would risk 2 threads both seeing "not present yet" and
    each creating their own separate RaydashCache for the same name. 
    */
    private final ConcurrentHashMap<String, Cache> cacheMap = new ConcurrentHashMap<>();

    public RaydashCacheManager(RaydashClient client, Integer defaultTTLSeconds) {
        this.client = client;
        this.defaultTTLSeconds = defaultTTLSeconds;
    }

    /*
    Raydash doesn't have any fixed, predeclared set of cache names to report up front.
    Every cache name just shows up for the first time something is @Cacheable with it,
    which is exactly what getMissingCache() below handles. So there's nothing to return
    here.
    */
    @Override
    protected Collection<? extends Cache> loadCaches() {
        return Collections.emptyList();
    }

    /*
    Called by Spring whenever it asks for a cache name that isn't already known.
    computeIfAbsent(String key, Function<? super String, ? extends Cache> mappingFunction)
    either returns the existing RaydashCache for that name, or automatically creates-and-stores
    a new one if this is the first time this name has been seen. 
    */
    @Override
    protected @Nullable Cache getMissingCache(String name) {
        return cacheMap.computeIfAbsent(name, cacheName -> new RaydashCache(cacheName, client, defaultTTLSeconds));
    }
}
