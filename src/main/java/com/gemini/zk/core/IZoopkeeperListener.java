package com.gemini.zk.core;

import org.apache.curator.framework.CuratorFramework;

/**
 * @author Will Wang 实现该接口用于监控
 * @version 创建时间：Jun 19, 2014 9:15:57 PM
 * 
 */
public interface IZoopkeeperListener {
	public void execute(CuratorFramework client);
	public void init(CuratorFramework client);
}
