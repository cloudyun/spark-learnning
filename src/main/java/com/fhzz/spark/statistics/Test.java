package com.fhzz.spark.statistics;

import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;

import com.fhzz.util.RedisUtil;

public class Test {

	public static void main(String[] args) {
		Jedis jedis = RedisUtil.getJedis();
		Map<String, String> all = jedis.hgetAll("ts");
		Set<String> set = all.keySet();
		for (String key : set) {
			System.out.println(key + ":" + all.get(key));
		}
	}

}
