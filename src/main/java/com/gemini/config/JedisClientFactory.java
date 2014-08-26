package com.gemini.config;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import redis.clients.jedis.JedisPoolConfig;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gemini.worker.HeartBeat;
/**
 * 
 * @date 2014-03-21
 * @update 2014-05-28
 * @author Will Wang
 * @mail flytowill@gmail.com
 * 用于获取客户端
 * 
 */
public class JedisClientFactory {
	
	List<JedisClient> clients = Lists.newArrayList();
	private static final Map<String, JedisClientFactory> map = Maps.newHashMap();
	JedisClient master = null;
	private int tried;
	
	/**
	 * 
	 * @param resource 读取配置文件名 
	 * redis.properties 则resource=redis
	 * 
	 */
	private JedisClientFactory(String resource){
		init(resource);
	}
	
	/**
	 * 防止实例化多个factory，一个resource只存在一个factory
	 * @param resource
	 * @return
	 */
	public static JedisClientFactory getFactory(String resource){
		JedisClientFactory f = map.get(resource);
		if (f == null) {
			synchronized (map) {
				if (f == null) {
					f = new JedisClientFactory(resource);
					map.put(resource, f);
				}
			}
		}
		return f;
	}
	
	/**
	 * 按照权重选取客户端
	 * @return
	 */
	private JedisClient round_robin() {
		JedisClient c = null, best = null;
		int m, n;
		int total = 0;
		for (int i = 0; i < clients.size(); i++) {
			n = i;	//当前位置
			m = 1 << i;
			if ((m & tried) > 0) {
				continue;
			} else {
				c = clients.get(i);
			}

			if (c.isDown()) {
				continue;
			}

			if (c.getFails() >= c.getMax_fails()) {
				continue;
			}
			c.setCurrent_weight(c.getCurrent_weight() + c.getEffective_weight());
			total += c.getEffective_weight();
			if (c.getEffective_weight() < c.getWeight()) {
				c.setEffective_weight(c.getEffective_weight() + 1);
			}
			if (best == null || best.getCurrent_weight() < c.getCurrent_weight()) {
				best = c;
			}
		}
		if (best == null) {
			return clients.get(0);
		}
		n = clients.indexOf(best);
		m = 1 << n;
		//位置重新置位
		tried &= m;
		best.setCurrent_weight(best.getCurrent_weight() - total);
		return best;
	}
	
	/**
	 * 随机选取一台client,从client中的pool取出一个Jedis使用
	 * 通常用于只读操作
	 * @return
	 */
	public JedisClient getClient() {
		JedisClient c;
		if (clients.size() == 1) {
			c = clients.get(0);
			if (c.isDown()) {
				// to be continue
			}
			return c;
		} else {
			return round_robin();
		}
	}
	
	/**
	 * 获取master client,通常用于写数据
	 * @return
	 */
	public JedisClient getMaster(){
		return master;
	}
	
	/**
	 * 初始化
	 */
	private void init(String resource) {
		ResourceBundle bundle = ResourceBundle.getBundle(resource);
		if (bundle == null) {
			throw new IllegalArgumentException(resource + ".properties not found!");
		}
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
		config.setMaxIdle(Integer.valueOf(bundle.getString("redis.pool.maxIdle")));
		config.setMaxWaitMillis(Long.valueOf(bundle.getString("redis.pool.maxWait")));
		config.setTestOnBorrow(Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow")));
		config.setTestOnReturn(Boolean.valueOf(bundle.getString("redis.pool.testOnReturn")));
		int maxtried = Integer.valueOf(bundle.getString("redis.connection.max.tried"));
		String clusters = bundle.getString("redis.clusters");
		if(!Strings.isNullOrEmpty(clusters)){
			String[] redis = clusters.split(",");
			for (String string : redis) {
				if (!Strings.isNullOrEmpty(string)) {
					String[] conf = string.split(":");
					JedisConfig jc = new JedisConfig(conf[0], Integer.valueOf(conf[1]), config, Integer.valueOf(conf[2]), maxtried);
					JedisClient c = new JedisClient(jc);
					clients.add(c);
				}
			}
		}
		try{
			String _master = bundle.getString("redis.master");
			if (!Strings.isNullOrEmpty(_master)) {
				String[] conf = _master.split(":");
				JedisConfig jc = new JedisConfig(conf[0], Integer.valueOf(conf[1]), config, Integer.valueOf(conf[2]), maxtried);
				master = new JedisClient(jc);
			}
		}catch(Exception e){
			
		}
		int blacktimeout = 10;
		try{
			String timeout = bundle.getString("black.list.time.out");
			if (!Strings.isNullOrEmpty(timeout)) {
				blacktimeout = Integer.valueOf(timeout);
				blacktimeout = blacktimeout > 0 ? blacktimeout : 10;
			}
		}catch(Exception e){
			
		}
		//创建心跳线程
		Thread thread = new Thread(new HeartBeat(clients, blacktimeout));
		thread.setDaemon(true);
		thread.setName("Jedis-checker-" + resource);
		thread.start();
	}

}
