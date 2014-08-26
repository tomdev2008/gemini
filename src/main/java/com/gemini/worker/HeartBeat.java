package com.gemini.worker;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.gemini.config.JedisClient;

/**
 * 
 * @date 2014-03-21
 * @author Will Wang
 * @mail flytowill@gmail.com 发送心跳
 */
public class HeartBeat implements Runnable {

	private List<JedisClient> list;
	private static Cache<Object, Object> brokenClient;
	private long last_send_time = 0l;

	public HeartBeat(List<JedisClient> list, int timeout) {
		this.list = list;
		brokenClient = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS).build();
	}

	public void run() {
		try {
			while (true) {
				for (int i = 0; i < list.size(); i++) {
					JedisClient c = list.get(i);
					if (brokenClient.getIfPresent(c) != null) {
						c.isAlive();
						if (c.isDown()) {
							brokenClient.put(c, true);
						}
					} else {
						// send message;
					}
				}
				// 发送报警短信
				if (brokenClient.size() > 0 && (System.currentTimeMillis() - last_send_time) > 5 * 60 * 1000l) {
					// send message
//					SMSUtil.sendSMS(Inet4Address.getLocalHost().getHostAddress() + "==redis error:" + brokenClient);
				}
				TimeUnit.MILLISECONDS.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
