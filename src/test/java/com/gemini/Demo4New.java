package com.gemini;

import java.util.concurrent.TimeUnit;

import com.gemini.core.MasterClient;
import redis.clients.jedis.Jedis;

import com.gemini.config.JedisClient;
import com.gemini.core.SlaveClients;
import com.gemini.zk.core.ZookeeperClient;

/** 
 * @author Will Wang
 * @version 创建时间：Jun 20, 2014 7:13:17 PM 
 * 
 */
public class Demo4New {
	
	static SlaveClients slaves = new SlaveClients("/redis-server/views/search/servers", "/redis-server/views/search/config");
	static MasterClient master = new MasterClient("/redis-server/views/master/servers", "/redis-server/views/master/config");

	static{
		ZookeeperClient.instance.addListener("/redis-server/views/search/servers", slaves);
        ZookeeperClient.instance.addListener("/redis-server/views/master/servers", master);
	}
	public static void main(String[] args) {
		while(true){
//			JedisClient client = slaves.getClient();
            JedisClient client = master.getMaster();
			try {
				Jedis j = client.getSource();
				if (j != null) {
					j.select(0);
					System.out.println(client +"\t" + j.ping());
				}
				TimeUnit.MILLISECONDS.sleep(10000);
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				//非常重要
				client.returnSource();
			}
		}
	}
}
