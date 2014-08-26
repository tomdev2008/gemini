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
public class MasterClient implements IZoopkeeperListener {
	private Logger LOG = (Logger) LoggerFactory.getLogger(this.getClass());
	private String serverPath;
	private String confPath;	//配置文件地址

	public MasterClient(String serverPath, String confPath) {
		this.serverPath = serverPath;
		this.confPath = confPath;
	}
	
	JedisClient master;

	@SuppressWarnings("resource")
	@Override
	public void execute(CuratorFramework client) {
		final PathChildrenCache cache = new PathChildrenCache(client, serverPath, false);
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
						JedisClient c = createClient(server, poolConfig, maxtried);
						master = c;
						LOG.info("server added:" + c.toString());
						break;
					case CHILD_REMOVED:
						changedPath = event.getData().getPath();
						server = ZKPaths.getNodeFromPath(changedPath);
						c = createClient(server, poolConfig, maxtried);
						master = null;
						LOG.info("server removed:" + c.toString());
						break;
					default:
						break;
				}
				LOG.info("servers:" + master);
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
			config.setMaxTotal(NumberUtils.toInt(map.get("redis.pool.maxActive"), 100));
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
	
	
	
	/**
	 * 随机选取一台client,从client中的pool取出一个Jedis使用
	 * 通常用于只读操作
	 * @return
	 */
	public JedisClient getMaster() {
		return master;
	}

	@Override
	public void init(CuratorFramework client) {
		int maxtried = loadMaxTried(client, confPath + "maxtried");
		JedisPoolConfig poolConfig = loadConfig(client);
		try{
			List<String> servers = client.getChildren().forPath(serverPath);
			if (servers.size() > 0) {
				JedisClient c = createClient(servers.get(0), poolConfig, maxtried);
				master = c;
			}
		}catch(Exception e){
			LOG.error("", e);
		}
	}

}
