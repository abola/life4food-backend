package life4food.backend.datasource;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class DataSourceCacheLoader {
	      
	static LoadingCache<String,Connection> cacheConnection = CacheBuilder
	        .newBuilder()
	        .expireAfterAccess(20, TimeUnit.SECONDS)
	        .build(new CacheLoader<String, Connection>(){
	            @Override
	            public Connection load(String key) throws Exception {        
	                //connect();      
	                return null;
	            } 
	            
	        });        
	
	public static Connection getCacheConnection(String connectionString){
		
		
		return null;
		
	}
	
	
	public static void init(){
		
		
		
	}
	
	
}
