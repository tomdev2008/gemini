package com.gemini;

import com.gemini.zk.core.ZookeeperClient;

/**
 * @author Will Wang
 * @version 创建时间：Jun 18, 2014 11:38:39 PM
 * 
 */
public class NodeChange {
	public static void main(String[] args) throws Exception{
		// client.start();
//		 client.setData().forPath("/redis-server/redisconf/test1", "aaa".getBytes());
//		byte[] b = client.getData().forPath("/redis-server/redisconf");
//		System.out.println(new String(b));
//		client.create().forPath("/redis-server/redisconf/test2");
//		client.delete().forPath("/redis-server/redisconf/test2");
		ZookeeperClient.instance.delete("/redis-server/views/search/servers/10.10.83.194:6379:3");
	}
}
