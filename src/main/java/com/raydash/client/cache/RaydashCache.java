package com.raydash.client.cache;

import java.util.concurrent.Callable;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.support.AbstractValueAdaptingCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.raydash.client.RaydashClient;

/*
This is the class that actually plugs raydash into Spring's caching abstraction.
Spring never calls RaydashClient directly from the appliation code (like project)
instead when a method is annotated @Cacheable, Spring's caching proxy intercepts
the call and talks to whichever Cache implementation is registered for that cache
name. This class IS the implementation.

AbstractValueAdaptingCache is a Spring base class that handles the "should a 
stored null be treated as a real cached value or as nothing cached at all" for us.
We just have to implement lookup/put/evict/clear and it takes care of wrapping/
unwrapping around that.

The rough call flow, so the methods below make sense in context:
                @Cacheable method called
                           |
                          \|/
            Spring asks this Cache for the key
                           |
                          \|/
                 lookup() is executed
                           |
                          \|/
  if it returns null, Spring runs your actual method body
  and calls put() with the result (cached for next time)
                           |
                          \|/
   next call with the same key, lookup() finds it and 
   returns and your method body never runs at all
*/
public class RaydashCache extends AbstractValueAdaptingCache {
    private static final Logger log = LoggerFactory.getLogger(RaydashCache.class);

    /*
    Why tf are we wrapping cached value in an envelope with its @type & @data like this?
    Raydash itself only knows how to store plain strings - it has no idea what a "User" 
    or an "Order" object is. So every value gets JSON encoded before it's sent, and read 
    back and decoded on the way out. The problem that forced @type field specifically:
    Jackson can serialize any object to JSON easily no problem, but deserializing needs
    to be told WHAT type to deserialize back into! Without that hint all Jackson can do
    is guess "well if it's anything it's gonna be a JSON Object!" and hands back a generic
    LinkedHashMap which then fails with a ClassCastException the moment calling code tries
    to treat it as the real DTO type. Storing the original class name alongside the data
    (@type) is what lets lookup() below reconstruct the exact original type instead of a
    generic map. 
    */
    private static final String TYPE_FIELD = "@type";
    private static final String DATA_FIELD = "@data";
    
    private final String name;
    private final RaydashClient client;
    private final Integer defaultTTLSeconds;
    private final ObjectMapper objectMapper; // Jackson's main entry point for turning Java objects into JSON and back

    public RaydashCache(String name, RaydashClient client, Integer defaultTTLSeconds) {
        /*
        allowNullValues = true here means Spring will still cache "this method returned 
        null for this key" as a real cached fact (via it's own internal NULL marker),
        rather than treating null as "nothing was cached" and calling the real method
        again every time
        */
        super(true); // allowNullValues = true
        this.name = name;
        this.client = client;
        this.defaultTTLSeconds = defaultTTLSeconds;

        this.objectMapper = new ObjectMapper();

        /*
        scans the classpath for Jackson modules (Java LocalDate/Instant/etc) and registers
        them automatically instead of us having to list every module by hand
        */
        this.objectMapper.findAndRegisterModules();
    }

    @Override 
    public String getName() { return name; }

    /* 
    Part of Spring's cache contract which let's advanced callers reach past the abstraction 
    and get underlying implementation directly if they ever need to.
    */
    @Override
    public Object getNativeCache() { return client; }

    /*
    lookup() is what Spring calls to check for a cache hit. Note there are 2 layers of 
    try/catch here, each catching a different kind of failure:
    - The OUTER catch is for anything going wrong in talking to raydash server itself
      like connection issues, timeout etc. logged as an error since this is worth knowing.
    - The INNER catch is specifically for "we got a value just fine from raydash but we
      couldn't make sense of it as JSON". logged as warning since a single bad/stale cache
      entry isn't nearly as serious as cache being unreachable or something.
    
    Either way end result is the same: treat it as cache miss and let the real method run.
    */
    @Override
    protected Object lookup(Object key) {
       try {
        /*
        Sort of similar to Redis, every cache gets it's own namespace on raydash by
        prefixing the key with it's Cache's name so cache "users" key 1 and cache
        "orders" key 1 never collide as the same raydash key.
        */
        String cacheKey = name + ":" + key.toString();
        String jsonValue = client.get(cacheKey);

        if(jsonValue == null) return null; // Raydash has no entry for this key

        try {
            JsonNode envelope = objectMapper.readTree(jsonValue);

            JsonNode typeNode = envelope.get(TYPE_FIELD);
            JsonNode dataNode = envelope.get(DATA_FIELD);

            if(typeNode == null || dataNode == null) {
                log.warn("[Raydash] Cache value for key: {} has no type metadata. Treating this as a cache miss", key);
                return null;
            }

            /*
            treeToValue is Jackson's "take this parsed JSON tree and turn it into actual instance of valueType"
            step. This is the exact reason and payoff we get for storing @type.
            */
            Class<?> valueType = resolveClass(typeNode.asText());
            return objectMapper.treeToValue(dataNode, valueType);    
        } catch (Exception parseException) {
            log.warn("[Raydash] Failed to deserialize cache value for key: {}. Treating as cache miss.", key);
            return null;
        }
       } catch (Exception e) {
        log.error("[Raydash] error on 'lookup' for key: {}", key, e);
        return null;
       }
    }  

    /*
    Now put() is called Spring itself right after a @Cacheable method actually ran (after a miss),
    so it can store the fresh result for next time.
    */
    @Override
    public void put(Object key, Object value) {
        // NOTE: allowNullValues handling above is a entirely separate mechanism from this
        if(value == null) return; // nothing meaningful to cache

        try {
            String cacheKey = name + ":" + key.toString();

            // build the envelope
            // ObjectNode is Jackson's mutable, buildable representation of a JSON object
            // As opposed to JsonNode which is a general read-only view used in lookup()
            ObjectNode envelope = objectMapper.createObjectNode();
            
            envelope.put(TYPE_FIELD, value.getClass().getName()); // Fully qualified class name: "com.example.User"
            envelope.set(DATA_FIELD, objectMapper.valueToTree(value)); // Value itself converted into a JSON tree

            String jsonValue = objectMapper.writeValueAsString(envelope);
            String response = client.set(cacheKey, jsonValue);

            // In case client.set() fails instead of silent discard we return whatever came from server
            // Turning silent failed writes into a visible warning in our logs
            if (response == null || !response.startsWith("+OK")) {
                log.warn("[Raydash] SET failed for key: {} — {}", key, response);
                return;
            }

            if(defaultTTLSeconds > 0) {
                client.expire(cacheKey, defaultTTLSeconds);
            }
        } catch (Exception e) {
            log.error("[Raydash] error on 'put' for key: {}", key);
        }
    }

    // expire not implemented because i guess we would never call it like this you know?

    // Called by Spring for @CacheEvict on a specific key
    @Override
    public void evict(Object key) {
        try {
            String cacheKey = name + ":" + key.toString();
            client.del(cacheKey);
        } catch (Exception e) {
            log.error("[Raydash] error on 'evict' for key: {}", key);
        }
    }

    // Called by Spring for @CacheEvict(allEntries = true) to wipe all caches on raydash server
    // This will ofcourse work for all namespaces as there is no concept of "only this cache name"
    @Override
    public void clear() {
        try {
            client.flushall();
        } catch (Exception e) {
            log.error("[Raydash] can't execute operation 'clear'");
        }    
    }    

    /*
    This is a different less common entry point from Spring's Cache interface. Spring @Cacheable
    never actually calls this overload it always goes through lookup()/put() above instead. This
    is safe to leave unimplemented for our use case.
    */
    @Override
    public <T> T get(Object arg0, Callable<T> arg1) { throw new UnsupportedOperationException("[Raydash] Instead of 'get' use 'lookup'"); }

    /*
    Turning a class name string like "com.example.User" back into an actual Class object so Jackson
    has something concrete to deserialize into.
    Wait why are we trying 2 different ClassLoaders here? 
    1. Thread.currentThread().getContextClassLoader() ("TCCL") is in a framework like Spring, often
       the loader that actually knows about YOUR application's classes. It can differ from the loader
       that loaded this very RaydashCache class itself, especially once you're inside an app server or
       a "fat jar" style deployement. Trying the TCCL first covers the common/expected cases.
    2. Faling back to Class's own loader covers the case where the TCCL is unavailable or doesn't know
       about the class for some reason. 2 attempts, not one! purely for robustness across different 
       ways this library might end up deployed.

    Basically:
    1. This loader is managed by your framework (Spring here) and is actively running your current task.
       That's why we said this loader is usually the only one that knows about YOUR application's custom classes.
    2. The loader that originally loaded the RaydashCache library itself. This one acts as a backup safety net.

    A fat JAR (also called an uber-JAR or executable JAR) is a single Java Archive file that bundles 
    an application's compiled code along with all of its third-party dependencies and libraries.
    */
    private Class<?> resolveClass(String className) throws ClassNotFoundException {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();

        if(tccl != null) {
            try {
                return Class.forName(className, true, tccl);
            } catch (ClassNotFoundException ignored) { }
        }
        return Class.forName(className, true, RaydashCache.class.getClassLoader());
    }
}
