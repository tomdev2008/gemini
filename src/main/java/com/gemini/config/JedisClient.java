package com.gemini.config;

import java.util.List;

import com.google.common.base.Strings;
import com.gemini.cache.CacheManager;
import com.gemini.cache.CacheType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
/**
 * 
 * @date 2014-03-21
 * @author Will Wang
 * @mail flytowill@gmail.com
 *
 */
public class JedisClient {
	private JedisConfig config;
	private JedisPool pool;
	private int current_weight; // 当前权重
	private int effective_weight; // 活动权重
	private int weight; // 初始权重
	private volatile boolean down; // 是否宕机
	private int max_fails = 10; // 最大错误次数(抛出异常次数)
	private int fails; // 错误次数
	private ThreadLocal<Jedis> jedis = new ThreadLocal<Jedis>();

	public JedisClient(JedisConfig config) {
		this.config = config;
		pool = new JedisPool(config.config, config.getHost(), config.getPort());
		this.weight = config.getWeight();
		this.current_weight = config.getWeight();
		this.effective_weight = config.getWeight();
		this.max_fails = config.getMax_fails();
	}
	
	/**
	 * 重置状态
	 */
	public void reset(){
		current_weight = weight;
		fails = 0;
		down = false;
	}

	/**
	 * 检查是否存活
	 * @return
	 */
	public boolean isAlive() {
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			String tick = j.ping();
			if ("PONG".equalsIgnoreCase(tick)) {
				if (down) {
					current_weight = weight;
					fails = 0;
					returnSource();
				}
				down = false;
				return true;
			} else {
				down = true;
				current_weight = 0;
				fails++;
			}
		} catch (JedisConnectionException e) {
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
			
		}
		return false;
	}
	
	public List<String> mGet(int db, String ...keys){
		Jedis j = jedis.get();
		boolean success = true;
		try {
			if (j == null) {
				j = pool.getResource();
			}
			j.select(db);
			return j.mget(keys);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
		return null;
	}
	
	public List<byte[]> mGet(int db, byte[]... keys) {
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			j.select(db);
			return j.mget(keys);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @param db
	 * @param key
	 * @return
	 */
	public String get(int db, String key) {
		Jedis j = null;
		try {
			j = pool.getResource();
			j.select(db);
			return j.get(key);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			pool.returnBrokenResource(j);
		} finally {
			if (j != null) {
				pool.returnResource(j);
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @param db
	 * @param key
	 * @return
	 */
	public String get(int db, String key, CacheType cacheType) {
		String ret = (String) CacheManager.get(cacheType.type, db + key);
		if (!Strings.isNullOrEmpty(ret)) {
			return ret;
		}
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			j.select(db);
			ret = j.get(key);
			if (!Strings.isNullOrEmpty(ret)) {
				CacheManager.set(cacheType.type, db + key, ret);
			}
			return ret;
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success =false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
		return null;
	}

	public byte[] get(int db, byte[] key) {
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			j.select(db);
			return j.get(key);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
		return null;
	}

	public void mSet(int db, String... keysvalues) {
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			j.select(db);
			j.mset(keysvalues);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
	}
	
	public void mSet(int db, byte[]... keysvalues) {
		boolean success = true;
		Jedis j = null;
		try {
			j = pool.getResource();
			j.select(db);
			j.mset(keysvalues);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
	}

	public void set(int db, String key, String value) {
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			j.select(db);
			j.set(key, value);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
	}
	
	public void set(int db, byte[] key, byte[] value) {
		Jedis j = null;
		boolean success = true;
		try {
			j = pool.getResource();
			j.select(db);
			j.set(key, value);
		} catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			success = false;
			pool.returnBrokenResource(j);
		} finally {
			if (j != null && success) {
				pool.returnResource(j);
			}
		}
	}
	
	public void setEx(int db, byte[] key, byte[] value,int expiretime) {
		Jedis j = null;
		try {
			j = pool.getResource();
			j.select(db);
        	j.setex(key, expiretime, value);
        }catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
		} finally {
			if (j != null) {
				pool.returnResource(j);
			}
		}
    }
	
    public void flushdb(int db){
    	Jedis j = null;
		try {
			j = pool.getResource();
			j.select(db);
        	j.flushDB();
        }catch (JedisConnectionException e) {
			e.printStackTrace();
			if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
		} finally {
			if (j != null) {
				pool.returnResource(j);
			}
		}
    }
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer().append(config.getHost());
		sb.append(":");
		sb.append(config.getPort());
		sb.append(":");
		sb.append(weight);
		sb.append("-");
		sb.append(current_weight + effective_weight);
		return sb.toString();
	}
	
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (config.getHost() + ":" + config.getPort()).hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof JedisClient))
			return false;
		JedisClient client = (JedisClient) obj;
		return client.config.getHost().equals(config.getHost()) && client.config.getPort() == config.getPort();
	}

	public int getCurrent_weight() {
		return current_weight;
	}

	public int getEffective_weight() {
		return effective_weight;
	}

	public int getWeight() {
		return weight;
	}

	public boolean isDown() {
		return down;
	}

	public int getMax_fails() {
		return max_fails;
	}

	public int getFails() {
		return fails;
	}

	public void setCurrent_weight(int current_weight) {
		this.current_weight = current_weight;
	}

	public void setEffective_weight(int effective_weight) {
		this.effective_weight = effective_weight;
	}

	/**
	 * 获得一个pool
	 * @return
	 */
	public JedisPool getPool() {
		return pool;
	}
	
	/**
	 * 释放source
	 * @param p
	 * @param j
	 */
	public void returnSource(Jedis j){
		if (j != null) {
			pool.returnResource(j);
		}
	}
	
	/**
	 * 获取一个jedis连接，并存放到threadlocal
	 * @return
	 */
	public Jedis getSource(){
		Jedis j = jedis.get();
		try {
			if (j == null) {
				j = pool.getResource();
			}
        }catch (JedisConnectionException e) {
        	if (fails >= max_fails) {
				down = true;
			}
			current_weight--;
			fails ++;
			pool.returnBrokenResource(j);
		} finally {
			jedis.set(j);
		}
		return j;
	}
	
	
	/**
	 * 将连接返回给jedispool
	 */
	public void returnSource(){
		Jedis j = jedis.get();
		if (j != null) {
			pool.returnResource(j);
		}
		jedis.remove();
	}
}
