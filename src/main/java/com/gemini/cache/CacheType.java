package com.gemini.cache;

/**
 * @author Will Wang
 * @version 创建时间：Jul 7, 2014 6:31:32 PM
 * 
 */
public enum CacheType {

	STAR("star_cache"), 
	SHOW("show_cache"), 
	VIEWS("views_cache"), 
	KIS("kis_cache"),
	KEYVALUE("keyvalue_cache");
	
	public String type;	//缓存的库名

	CacheType(String type) {
		this.type = type;
	}
}
