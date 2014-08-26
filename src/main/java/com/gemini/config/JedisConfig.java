package com.gemini.config;

import redis.clients.jedis.JedisPoolConfig;

public class JedisConfig {
	
	private String host;
	private int port;
	JedisPoolConfig config;
	private int effective_weight; // 活动权重
	private int weight; // 初始权重
	private int max_fails = 100; // 最大错误次数(抛出异常次数)

	/**
	 * 
	 * @param host
	 * @param port
	 * @param config
	 * @param weight
	 * @param maxfail
	 */
	public JedisConfig(String host, int port, JedisPoolConfig config, int weight, int maxfail) {
		this.host = host;
		this.port = port;
		this.config = config;
		this.weight = weight;
		this.effective_weight = weight;
		this.max_fails = maxfail;
	}

	public int getEffective_weight() {
		return effective_weight;
	}

	public int getWeight() {
		return weight;
	}

	public int getMax_fails() {
		return max_fails;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
}
