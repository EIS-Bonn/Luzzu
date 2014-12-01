package de.unibonn.iai.eis.luzzu.cache;

import java.io.File;
import java.io.IOException;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import de.unibonn.iai.eis.luzzu.properties.PropertyManager;

/**
 * @author Jeremy Debattista
 *
 * The class Cache is a wrapper class for the LRUMap object
 * to enable the use of caching in the application.
 * 
 * The objects in this class are thread-safe.
 */
class Cache {

	private DB db;
	private String name;
	private HTreeMap<Object, CacheObject> cache;

	/**
	 * Initialise a new cache with an identifier name and the maximum amount of objects allowed.
	 * 
	 * @param name - Cache Identifier
	 * @param maxItems - Number of object allowed
	 */
	protected Cache(String name, int maxItems){
		this.name = name;
		
		File tempFolder = new File("tmp/caches/");
		if (!tempFolder.exists()) tempFolder.mkdirs();
		
		
		File tempFile = new File("tmp/caches/luzzu_"+name);
		if (!tempFile.exists()){
			try {
				tempFile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		this.db = DBMaker.newFileDB(tempFile)
				.cacheLRUEnable()
				.cacheSize(Integer.parseInt(PropertyManager.getInstance().getProperties("cache.properties").getProperty("CACHE_SIZE_IN_GB")))
				.cacheSize(maxItems).make();
		this.cache = db.getHashMap("cache_"+name);
	}
	
	/**
	 * Adds item to the cache.
	 * 
	 * @param key - An identifiable key for the item added
	 * @param value - The item added
	 */
	protected void addToCache(Object key, CacheObject value){
		this.cache.put(key, value);
	}
	
	/**
	 * Fetches item from the cache and marks it as the most recently used.
	 * 
	 * @param key - The identifiable key
	 * @return Returns the object from cache
	 */
	protected CacheObject getFromCache(Object key){
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
		this.db.getEngine().clearCache();
		this.cache.clear();
	}
	
	/**
	 * Close all cache resources
	 */
	@Override
	protected void finalize() throws Throwable {
		try {
			this.cache.close();
			this.db.close();
		} catch(Throwable ex) {
			//TODO: log exception
		} finally {
			super.finalize();
		}
	}
}