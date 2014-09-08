package de.unibonn.iai.eis.luzzu.cache;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.collections4.map.LRUMap;

/**
 * @author Jeremy Debattista
 *
 * The class Cache is a wrapper class for the LRUMap object
 * to enable the use of caching in the application.
 * 
 * The objects in this class are thread-safe.
 */
class Cache {

	private String name;
	private Map<Object, Object> cache;

	/**
	 * Initialise a new cache with an identifier name and the maximum amount of objects allowed.
	 * 
	 * @param name - Cache Identifier
	 * @param maxItems - Number of object allowed
	 */
	protected Cache(String name, int maxItems){
		this.cache = Collections.synchronizedMap(new LRUMap<Object, Object>(maxItems, true));
		this.name = name;
	}
	
	/**
	 * Adds item to the cache.
	 * 
	 * @param key - An identifiable key for the item added
	 * @param value - The item added
	 */
	protected void addToCache(Object key, Object value){
		this.cache.put(key, value);
	}
	
	/**
	 * Fetches item from the cache and marks it as the most recently used.
	 * 
	 * @param key - The identifiable key
	 * @return Returns the object from cache
	 */
	protected Object getFromCache(Object key){
		return this.cache.get(key);
	}
	
	/**
	 * Checks if a particular object exists in the cache
	 * 
	 * @param key - The identifiable key
	 * @return Returns true if the object exists in the cache.
	 */
	protected boolean existsInCache(Object key){
		return this.cache.containsKey(key);
	}
	
	/**
	 * @return Returns the Cache Identifier Name
	 */
	protected String getName(){
		return this.name;
	}
	
	/**
	 * Clears memory from unused resources
	 */
	protected void cleanup(){
		this.cache.clear();
		this.cache = null;
	}
}
