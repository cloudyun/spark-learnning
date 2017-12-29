package com.fhzz.spark.petition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;

public class Petition {
	
	private static String checkpointDir = "petition-checkpoint";
	
	public static void main(String[] args) {
		// 接收数据的地址和端口
		String zkQuorum = "10.2.112.209:2181";
		// 话题所在的组
		String group = "1";
		// 话题名称以“，”分隔
		String topics = "face";
		// 每个话题的分片数
		int numThreads = 2;

		SparkConf conf = new SparkConf();
		/*
		 * 配置应用名称以及配置两个线程
		 */
		conf.setAppName("petition_in_real_time");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		jssc.checkpoint(checkpointDir);
		
		
		// 存放话题跟分片的映射关系
		Map<String, Integer> topicmap = new HashMap<String, Integer>();
		String[] topicsArr = topics.split(",");                          
		for (int i = 0; i < topicsArr.length; i++) {
			topicmap.put(topicsArr[i], numThreads);
		}

		// 从Kafka中获取数据转换成RDD
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap);
		
		//转换为FaceData
		JavaDStream<FaceData> map = messages.map(new Function<Tuple2<String,String>, FaceData>() {

			private static final long serialVersionUID = 1L;

			@Override
			public FaceData call(Tuple2<String, String> value) throws Exception {
				FaceData data = JSONObject.parseObject(value._2, FaceData.class);
				Connection con = ConnectionPool.getConnection();
				String sql = "insert into face (name, age) values (?, ?)";
				PreparedStatement ps = con.prepareStatement(sql);
				int x = 0;
				ps.setString(x++, data.getName());
				ps.setInt(x++, data.getAge());
				ps.executeUpdate();
				ps.close();
				return data;
			}
		});
		
		//过滤数据
		JavaDStream<FaceData> filter = map.filter(new Function<FaceData, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(FaceData face) throws Exception {
				return face.getAge() > 20;
			}
		});
		
		
		JavaDStream<FaceData> window = filter.reduceByWindow(new Function2<FaceData, FaceData, FaceData>() {

			private static final long serialVersionUID = 1L;

			@Override
			public FaceData call(FaceData a, FaceData b) throws Exception {
				return a;
			}
		}, new Duration(10000), new Duration(10000));
		
		//处理数据
		window.foreachRDD(new VoidFunction<JavaRDD<FaceData>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<FaceData> rdd) throws Exception {
				
				rdd.foreach(new VoidFunction<FaceData>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public void call(FaceData data) throws Exception {
						Connection con = ConnectionPool.getConnection();
						String sql = "insert into face (name, age) values (?, ?)";
						PreparedStatement ps = con.prepareStatement(sql);
						int x = 0;
						ps.setString(x++, data.getName());
						ps.setInt(x++, data.getAge());
						ps.executeUpdate();
						ps.close();
					}
				});
				
//				rdd.foreachPartition(new VoidFunction<Iterator<FaceData>>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void call(Iterator<FaceData> values) throws Exception {
//						while (values.hasNext()) {
//							FaceData data = values.next();
//							Connection con = ConnectionPool.getConnection();
//							String sql = "insert into face (name, age) values (?, ?)";
//							PreparedStatement ps = con.prepareStatement(sql);
//							int x = 0;
//							ps.setString(x++, data.getName());
//							ps.setInt(x++, data.getAge());
//							ps.executeUpdate();
//							ps.close();
//						}
//					}
//				});
			}
		});
		
		window.print();
		
		jssc.start();
		jssc.awaitTermination();
	}
}