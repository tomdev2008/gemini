package com.gemini.zk.core;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemini.util.IPUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/** 
 * @author Will Wang
 * @version 创建时间：Jul 14, 2014 6:16:28 PM 
 * 
 */
public class ZookeeperClient {
	private static final String zkConnectionString = "10.10.53.20:2181,10.10.53.21:2181,10.10.53.93:2181";
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private CuratorFramework zkClient;
    private ConcurrentHashMap<String, IZoopkeeperListener> listeners = new ConcurrentHashMap<String, IZoopkeeperListener>();
    
    public static final ZookeeperClient instance = new ZookeeperClient();
    public void addListener(String path, IZoopkeeperListener listener){
    	listener.execute(zkClient);
    	try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
    	listeners.put(path, listener);
    	if (!Strings.isNullOrEmpty(path)) {
    		String clientPath = path.replace("servers", "clients");
			String ip =  IPUtils.getIp();
			if (!Strings.isNullOrEmpty(ip)) {
				createEphemeral(clientPath + "/" + ip + ":" +System.currentTimeMillis());
				logger.info("客户端ip:"+ ip + ",注册成功!");
			} else {
				logger.error("获取本机IP失败");
			}
			
		}
    }
    
    private ZookeeperClient(){
    	//1000 是重试间隔时间基数，3 是重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = createWithOptions(zkConnectionString, retryPolicy, 2000, 10000);
        zkClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                logger.info("CuratorFramework state changed: {}", newState);
                if(newState == ConnectionState.CONNECTED || newState == ConnectionState.RECONNECTED){
                	for (String key : listeners.keySet()) {
                		System.out.println(key);
						IZoopkeeperListener listener = listeners.get(key);
						listener.execute(client);
					}
                }
            }
        });
        zkClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
            @Override
            public void unhandledError(String message, Throwable e) {
                logger.info("CuratorFramework unhandledError: {}", message);
            }
        });
        zkClient.start();
    }
    
    public CuratorFramework  createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs){
        return CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();
    }

    public void create(String path) {
		try {
			zkClient.create().forPath(path);
		} catch (NodeExistsException e) {
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	public void createEphemeral(String path) {
		try {
			zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
		} catch (NodeExistsException e) {
			logger.error("", e);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	public void delete(String path) {
		try {
			zkClient.delete().forPath(path);
		} catch (NoNodeException e) {
			logger.error("", e);
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	public List<String> getChildren(String path) {
		try {
			return zkClient.getChildren().forPath(path);
		} catch (NoNodeException e) {
			return Lists.newArrayList();
		} catch (Exception e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	public boolean isConnected() {
		return zkClient.getZookeeperClient().isConnected();
	}

	public void doClose() {
		zkClient.close();
	}
}
