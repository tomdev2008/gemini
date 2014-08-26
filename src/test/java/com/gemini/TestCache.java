package com.gemini;

import com.gemini.cache.CacheManager;

/**
 * Author: Will Wang
 * Date: 7/25/14
 * Time: 3:25 PM
 * Usage:
 */
public class TestCache {
    public static void main(String[] args) {
        CacheManager.set("views_cache", "中国" , "美国");
        System.out.println(CacheManager.get("views_cache","中国"));
    }
}
