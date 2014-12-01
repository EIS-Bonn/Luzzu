package de.unibonn.iai.eis.luzzu.cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Jeremy Debattista
 * 
 * The CacheManager class handles all caching operations.
 * The Manager allows for the creation of new thread-safe 
 * caches in the system. 
 * The CacheManager controls all operations in the cache,
 * expiring items based on an LRU algorithm.
 * 
 * The CacheManager is a singleton class and can be invoked
 * via the {@link #getInstance()} method.
 */
public class CacheManager{

	
	private ConcurrentHashMap<String, Cache> registeredCache = new ConcurrentHashMap<String, Cache>();
	private static CacheManager instance = null;
	
	protected CacheManager(){}
	
	/**
	 * Create or return a singleton instance of the CacheManager
	 * 
	 * @return CacheManager instance
	 */
	public static CacheManager getInstance(){
		if (instance == null){
			instance = new CacheManager();
		}
		return instance;
	}
	
	/**
	 * Creates a new cache in the cache manager.
	 * 
	 * @param cacheName - Name given to the required cache
	 * @param maxItems - Max number of items stored in the cache
	 */
	public void createNewCache(String cacheName, int maxItems) {
		registeredCache.put(cacheName, new Cache(cacheName, maxItems));
	}
	
	/**
	 * Adds item to a cache.
	 * 
	 * @param cacheName - Cache identifier name to which item is being added
	 * @param key - An identifiable key for the item added
	 * @param value - The cache object added
	 */
	public void addToCache(String cacheName, Object key, CacheObject value){
		registeredCache.get(cacheName).addToCache(key, value);
	}
	

	/**
	 * Fetches item from a cache and marks it as the most recently used.
	 * 
	 * @param cacheName - Cache identifier name to which item is being fetched
	 * @param key - The identifiable key
	 * @return Returns the object from cache
	 */
	public CacheObject getFromCache(String cacheName, Object key){
		return registeredCache.get(cacheName).getFromCache(key);
	}
	
	/**
	 * Checks if cache exitsts
	 * 
	 * @param cacheName
	 * @return true if cache exists
	 */
	public boolean cacheExists(String cacheName){
		return registeredCache.containsKey(cacheName);
	}
	
	/**
	 * Checks if a particular object exists in a cache
	 * 
	 * @param cacheName - Cache identifier name to which item is being fetched
	 * @param key - The identifiable key
	 * @return Returns true if the object exists in the cache.
	 */
	public boolean existsInCache(String cacheName,Object key){
		return registeredCache.get(cacheName).existsInCache(key);
	}
	
	
	/**
	 * This method should be invoked to clear up resources from memory
	 */
	public void cleanup(){
		for(String key : registeredCache.keySet()){
			registeredCache.get(key).cleanup();
		}
		registeredCache.clear();
	}
}
