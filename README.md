Gemini Project
What's Gemini
Gemini是集成的redis客户端，对于主从结构的redis环境可以按照权重选取服务器返回,如果取数据过程发生异常过多则将权重降低.超过最大错误次数时就将服务器下线.等恢复之后重新设置初始权重并重新上线.目前支持黑名单，可以设置黑名单的时间


Version 1.0
===========
What's New:
1.增加zookeeper管理集群，可以根据不同的路径来管理不同的集群。相互调用机器不受彼此影响。可以动态加载新增机器，下线机器。
2.集成bigmemory，可以增加本地缓存，bigmemory的key需要自己申请，添加到classpath即可
3.client增加新的方法。可以选择是否使用本地缓存

===========
最新demo
1.在zookeeper集群上创建相应目录
```java
	ZookeeperClient.instance.create("/redis-server/views/search/servers/10.10.83.194:6379:3");
```
2.调用方法如下
```java
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
				client.returnSource();
			}
		}
	}
}
```

Verison 0.9
============

新建配置文件redis.properties
放到src目录下即可。
可以创建多个redis集群的数据，创建多个properties并调用JedisClientFactory.getFactory("redis")方法即可创建

格式如下:

```
#最大分配的对象数  
redis.pool.maxActive=1024
#最大能够保持idle状态的对象数
redis.pool.maxIdle=200
#当池内没有返回对象时，最大等待时间
redis.pool.maxWait=1000
#当调用borrow Object方法时，是否进行有效性检查
redis.pool.testOnBorrow=true
#当调用return Object方法时，是否进行有效性检查
redis.pool.testOnReturn=true
#以逗号分开,每个redis的格式为ip:port:weight
redis.clusters=127.0.0.1:6379:12,127.0.0.1:6378:12,127.0.0.1:6377:15
#最大重试次数之后进入黑名单
redis.connection.max.tried=10
#redis master，通常可以用来写数据
redis.master=127.0.0.1:6379:12
#异常服务器进入黑名单的时间。超过时间后则自动重试。单位秒，默认时间10s
black.list.time.out=10
```


```java
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
```
