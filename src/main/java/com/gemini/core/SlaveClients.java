package com.gemini.core;

import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gemini.config.JedisClient;
import com.gemini.config.JedisConfig;
import com.gemini.util.NumberUtils;
import com.gemini.zk.core.IZoopkeeperListener;

/**
 * @author Will Wang
 * @version 创建时间：Jun 19, 2014 9:26:16 PM
 * 
 */
public class SlaveClients implements IZoopkeeperListener {
	private Logger LOG = (Logger) LoggerFactory.getLogger(this.getClass());
	private String serverPath;
	private String confPath;	//配置文件地址

	public SlaveClients(String serverPath, String confPath) {
		this.serverPath = serverPath;
		this.confPath = confPath;
	}
	
	
	List<JedisClient> clients = Lists.newArrayList();

	@SuppressWarnings("resource")
	@Override
	public void execute(CuratorFramework client) {
		PathChildrenCache cache = new PathChildrenCache(client, serverPath, true);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework zkClient, PathChildrenCacheEvent event) throws Exception {
				int maxtried = loadMaxTried(zkClient, serverPath + "maxtried");
				JedisPoolConfig poolConfig = loadConfig(zkClient);
				String changedPath = null;
				switch (event.getType()) {
					case CHILD_ADDED:
						changedPath = event.getData().getPath();
						String server = ZKPaths.getNodeFromPath(changedPath);
						if (server.equals("config")) {
							break;
						}
						JedisClient c = createClient(server, poolConfig, maxtried);
						synchronized (clients) {
							clients.add(c);
							LOG.info("server added:" + c.toString());
						}
						break;
					case CHILD_REMOVED:
						changedPath = event.getData().getPath();
						server = ZKPaths.getNodeFromPath(changedPath);
						c = createClient(server, poolConfig, maxtried);
						synchronized (clients) {
							clients.remove(c);
							LOG.info("server removed:" + c.toString());
						}
						break;
					default:
						break;
				}
				LOG.info("server size: "+ clients.size() +", servers:" + clients);
			}
		});
        try {
            cache.start();
        } catch (Exception e) {
        	LOG.error("Start PathChildrenCache error for path: {}, error info: {}", serverPath, e.getMessage());
        }
	}
	
	private JedisPoolConfig loadConfig(CuratorFramework client){
		JedisPoolConfig config = new JedisPoolConfig();
		Map<String, String> map = Maps.newHashMap();
		try {
			List<String> confs = client.getChildren().forPath(confPath);
			for (String conf : confs) {
				byte[] bytes = client.getData().forPath(confPath + conf);
				map.put(conf, new String(bytes));
			}
			
			config.setMaxTotal(NumberUtils.toInt(map.get("redis.pool.maxActive"), 20));
			config.setMaxIdle(NumberUtils.toInt(map.get("redis.pool.maxIdle"),10));
			config.setMaxWaitMillis(NumberUtils.toLong(map.get("redis.pool.maxWait"), 1000));
			config.setTestOnBorrow(NumberUtils.toBoolean(map.get("redis.pool.testOnBorrow"), true));
			config.setTestOnReturn(NumberUtils.toBoolean(map.get("redis.pool.testOnReturn"), true));
		} catch (Exception e) {
//			LOG.error("", e);
		}
		return config;
	}

	private int loadMaxTried(CuratorFramework client, String path){
		int maxtried = 10;
		try {
			byte[] maxbytes = client.getData().forPath(path);
			if (maxbytes != null) {
				maxtried = NumberUtils.toInt(new String() , 10);
			}
		} catch (Exception e) {
//			LOG.error("", e);
		}
		return maxtried;
	}
	
	private JedisClient createClient(String server,JedisPoolConfig poolConfig, int maxtried){
		String[] conf = server.split(":");
		JedisConfig jc = new JedisConfig(conf[0], Integer.valueOf(conf[1]), poolConfig, Integer.valueOf(conf[2]), maxtried);
		JedisClient c = new JedisClient(jc);
		return c;
	}
	
	
	private int tried;
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

	@Override
	public void init(CuratorFramework client) {
		int maxtried = loadMaxTried(client, confPath + "maxtried");
		JedisPoolConfig poolConfig = loadConfig(client);
		try{
			List<JedisClient> list = Lists.newArrayList();
			List<String> servers = client.getChildren().forPath(serverPath);
			for (String server : servers) {
				if (server.equals("config")) {
					continue;
				}
				JedisClient c = createClient(server, poolConfig, maxtried);
				LOG.info("server added:" + c.toString());
				list.add(c);
			}
			if (list.size() > 0) {
				synchronized (clients) {
					clients = list;
					list = null;
				}
			}
		}catch(Exception e){
			LOG.error("", e);
		}
	}

}
