package com.fhzz.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import com.alibaba.fastjson.JSON;

/**
 * @FileName : (RedisUtil.java)
 * 
 * @description : TODO(这里用一句话描述这个类的作用)
 * @author: gaoyun
 * @version: Version No.1
 * @date: 2017年10月11日
 * @modify: 2017年10月11日 上午9:22:52
 * @copyright: FiberHome FHZ Telecommunication Technologies Co.Ltd.
 *
 */
public class RedisUtil implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2929604567591839330L;

	private static final Logger LOG = Logger.getLogger(RedisUtil.class);
	
	private static JedisPool jedisPool = null;
	
	private static ShardedJedisPool shardedJedisPool = null;
	
	/**
	 * 初始化Redis连接池
	 */
	static {
		try {
			// 加载redis配置文件
			ResourceBundle bundle = ResourceBundle.getBundle("config.jdbc");
			if (bundle == null) {
				throw new IllegalArgumentException("[config/jdbc.properties] is not found!");
			}
			int maxActivity = Integer.valueOf(bundle.getString("redis.pool.maxActive"));
			int maxIdle = Integer.valueOf(bundle.getString("redis.pool.maxIdle"));
			long maxWait = Long.valueOf(bundle.getString("redis.pool.maxWait"));
			boolean testOnBorrow = Boolean.valueOf(bundle.getString("redis.pool.testOnBorrow"));
			boolean onreturn = Boolean.valueOf(bundle.getString("redis.pool.testOnReturn"));
			// 创建jedis池配置实例
			JedisPoolConfig config = new JedisPoolConfig();
			// 设置池配置项值
			config.setMaxTotal(maxActivity);
			config.setMaxIdle(maxIdle); // 最大空闲连接数
			config.setMaxWaitMillis(maxWait);
			config.setTestOnBorrow(testOnBorrow);
			config.setTestOnReturn(onreturn);
			String host = bundle.getString("redis.host");
			Integer port = Integer.valueOf(bundle.getString("redis.port"));
//			String password = bundle.getString("redis.password");
//			jedisPool = new JedisPool(config, host, port, 10000, password);
			jedisPool = new JedisPool(config, host, port, 10000);
			// slave链接
			List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
//			shards.add(new JedisShardInfo(host, Integer.valueOf(bundle.getString("redis.port1"))));
			Integer masterPort = Integer.valueOf(bundle.getString("redis.port1"));
			shards.add(new JedisShardInfo(host, masterPort, "master"));
			shardedJedisPool = new ShardedJedisPool(config, shards);
			LOG.info("初始化Redis连接池success");
		} catch (Exception e) {
			LOG.error("初始化Redis连接池 出错！", e);
		}
	}

	/**
	 * 获取Jedis实例
	 * 
	 * @return
	 */
	public synchronized static Jedis getJedis() {
		try {
			if (jedisPool != null) {
				return jedisPool.getResource();
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Redis缓存获取Jedis实例 出错！", e);
			return null;
		}
	}

	/**
	 * 获取shardedJedis实例
	 * 
	 * @return
	 */
	public static ShardedJedis getShardedJedis() {
		try {
			if (shardedJedisPool != null) {
				return shardedJedisPool.getResource();
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Redis缓存获取shardedJedis实例 出错！", e);
			return null;
		}
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public static void returnResource(final Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}

	/**
	 * 释放shardedJedis资源
	 * 
	 * @param jedis
	 */
	public static void returnResource(final ShardedJedis shardedJedis) {
		if (shardedJedis != null) {
			shardedJedis.close();
		}
	}

	/**
	 * 向缓存中设置字符串内容
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @return
	 * @throws Exception
	 */
	public static boolean set(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			if (jedis != null) {
				jedis.set(key, value);
			}
			return true;
		} catch (Exception e) {
			LOG.error("Redis缓存设置key值 出错！", e);
			return false;
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 判断key是否存在
	 */
	public static boolean exists(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = getShardedJedis();
			if (jedis == null) {
				return false;
			} else {
				return jedis.exists(key);
			}
		} catch (Exception e) {
			LOG.error("Redis缓存判断key是否存在 出错！", e);
			return false;
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 删除缓存中的对象，根据key
	 * 
	 * @param key
	 * @return
	 */
	public static boolean del(String key) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			jedis.del(key);
			return true;
		} catch (Exception e) {
			LOG.error("Redis del key:[" + key + "]出错！", e);
			return false;
		} finally {
			returnResource(jedis);
		}
	}

	// *******************key-value****************start

	/**
	 * 向缓存中设置对象
	 * 
	 * @param key
	 * @param value
	 * @return
	 */
	public static boolean set(String key, Object value) {
		Jedis jedis = null;
		try {
			String objectJson = JSON.toJSON(value).toString();
			jedis = getJedis();
			if (jedis != null) {
				jedis.set(key, objectJson);
			}
			return true;
		} catch (Exception e) {
			LOG.error("Redis set key:[" + key + "]出错！", e);
			return false;
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 根据key 获取内容
	 * 
	 * @param key
	 * @return
	 */
	public static Object get(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = shardedJedisPool.getResource();
			Object value = jedis.get(key);
			return value;
		} catch (Exception e) {
			LOG.error("Redis get key:[" + key + "]出错！", e);
			return false;
		} finally {
			returnResource(jedis);
		}
	}

	/**
	 * 根据key 获取对象
	 * 
	 * @param key
	 * @return
	 */
	public static <T> T get(String key, Class<T> clazz) {
		ShardedJedis jedis = null;
		try {
			jedis = getShardedJedis();
			if (jedis != null) {
				return (T) JSON.toJavaObject(JSON.parseObject(jedis.get(key)), clazz);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Redis get key:[" + key + "]出错！", e);
			return null;
		} finally {
			returnResource(jedis);
		}
	}

	// *******************key-value****************end

	// *************** 操作list****************start
	/**
	 * 向缓存中设置对象
	 * 
	 * @param key
	 * @param list
	 *            T string calss
	 * @return
	 */
	public static <T> boolean setList(String key, List<T> list) {
		Jedis jedis = null;
		try {
			jedis = getJedis();
			if (jedis != null) {
				for (T vz : list) {
					if (vz instanceof String) {
						jedis.lpush(key, (String) vz);
					} else {
						String objectJson = JSON.toJSON(vz).toString();
						jedis.lpush(key, objectJson);
					}
				}
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			LOG.error("Redis setList key:[" + key + "]出错！", e);
			return false;
		} finally {
			returnResource(jedis);
		}
	}


	public static List<String> getListString(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = getShardedJedis();
			if (jedis != null) {
				return jedis.lrange(key, 0, -1);
			} else {
				return null;
			}
		} catch (Exception e) {
			LOG.error("Redis getListString key:[" + key + "]出错！", e);
			return null;
		} finally {
			returnResource(jedis);
		}
	}

	// *************** 操作list****************end

	
}
