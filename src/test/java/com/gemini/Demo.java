package com.gemini;

import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;

import com.gemini.config.JedisClient;
import com.gemini.config.JedisClientFactory;

/** 
 * @author Will Wang
 * @version 创建时间：Apr 9, 2014 11:42:38 AM 
 * 
 */
public class Demo {
	//字符串为*.properties的*部分
	private static final JedisClientFactory factory = JedisClientFactory.getFactory("redis");
	public static void main(String[] args) {
		while(true){
			JedisClient client = factory.getClient();
			try {
				Jedis j = client.getSource();
				if (j != null) {
					j.select(0);
					System.out.println(client +"\t" + j.ping());
				}
				TimeUnit.MILLISECONDS.sleep(100);
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				//非常重要
				client.returnSource();
			}
		}
	}
}
