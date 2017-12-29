package com.fhzz.spark.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import redis.clients.jedis.ShardedJedis;
import scala.Tuple2;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fhzz.util.RedisUtil;

/**
 * @FileName : (TrafficStatistics.java) 
 * 
 * @description  : (车流量统计)
 * @author: gaoyun
 * @version: Version No.1
 * @date: 2017年12月28日
 * @modify: 2017年12月28日 下午1:37:08
 * @copyright: FiberHome FHZ Telecommunication Technologies Co.Ltd.
 *
 */
public class TrafficStatistics {
	
	public final static String INVALID = "'-','?','无牌','00000000','XXXXXXX','11111111','0','车牌','未知','未识别','无车牌'";
	public final static String STATISTICSKEY = "ts";

	
	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: TrafficStatistics <zk> <group> <topics> <numThreads>");
			System.exit(1);
		}
		
		String zk = args[0];
		
		String group = args[1];
		
		String[] topics = args[2].split(",");
		
		int numThreads = Integer.parseInt(args[3]);
		
		Map<String, Integer> topicmap = new HashMap<String, Integer>();
		for (String topic : topics) {
			topicmap.put(topic, numThreads);
		}
		
		SparkConf conf = new SparkConf();
		conf.setAppName("Traffic_Statistics");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(2000));
		JavaPairReceiverInputDStream<String, String> cs = KafkaUtils.createStream(jsc, zk, group, topicmap);
		
		JavaDStream<String> map = cs.map(new Function<Tuple2<String,String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, String> value) throws Exception {
				JSONObject json = JSON.parseObject(value._2);
				String hphm = json.getOrDefault("HPHM", "").toString();
				String puid = json.getOrDefault("PUID", "").toString();
				return puid + "=" + hphm;
			}
		});
		
		//过滤无效数据
		JavaDStream<String> filter = map.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String value) throws Exception {
				String hphm = value.split("=")[1];
				if (hphm == null) {
					return false;
				}
				if (INVALID.indexOf(hphm) >= 0) {
					return false;
				}
				return true;
			}
		});
		
		filter.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> values) throws Exception {
				List<String> collect = values.collect();
				ShardedJedis shardedJedis = RedisUtil.getShardedJedis();
				for (String value : collect) {
					if (shardedJedis == null) {
						continue;
					}
					shardedJedis.hincrBy(STATISTICSKEY, value.split("=")[0], 1);
				}
			}
		});
		
		jsc.start();
		jsc.awaitTermination();
	}
}
