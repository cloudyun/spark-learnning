package com.fhzz.spark.wordcount;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class WordCount {
	
	public static void main(String[] args) throws FileNotFoundException {
		
		// 设置匹配模式，以空格分隔
		final Pattern SPACE = Pattern.compile(" ");
		// 接收数据的地址和端口
		String zkQuorum = "10.2.112.209:2181";
		// 话题所在的组
		String group = "1";
		// 话题名称以“，”分隔
		String topics = "words";
		// 每个话题的分片数
		int numThreads = 2;

		SparkConf conf = new SparkConf();
		/*
		 * 配置应用名称以及配置两个线程
		 */
		conf.setAppName("WordCountOnline");
		
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));
		// 存放话题跟分片的映射关系
		Map<String, Integer> topicmap = new HashMap<String, Integer>();
		String[] topicsArr = topics.split(",");
		for (int i = 0; i < topicsArr.length; i++) {
			topicmap.put(topicsArr[i], numThreads);
		}

		// 从Kafka中获取数据转换成RDD
		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap);

		// 从话题中过滤所需数据
		JavaDStream<String> map = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
			
			private static final long serialVersionUID = 1L;
			
			public Iterable<String> call(Tuple2<String, String> value) throws Exception {
				return Lists.newArrayList(SPACE.split(value._2));
			}
		});
		
//		JavaPairDStream<String, Integer> pairs = map.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Integer>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<Tuple2<String, Integer>> call(Iterator<String> values) throws Exception {
//				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
//				while (values.hasNext()) {
//					String next = values.next();
//					list.add(new Tuple2<String, Integer>(next, 1));
//				}
//				return list;
//			}
//		});
		
		JavaPairDStream<String, Integer> pairs = map.mapToPair(new PairFunction<String, String, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String value) {
				return new Tuple2<String, Integer>(value.toLowerCase(), 1);
			}
			
		});
		
//		pairs.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//			
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(JavaPairRDD<String, Integer> value) throws Exception {
//				System.out.println(value.collect());
//			}
//		});
		JavaPairDStream<String, Integer> reduce = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer a, Integer b) throws Exception {
				return a + b;
			}
		}, new Duration(6000));
		
		reduce.print();

		jssc.start();

		jssc.awaitTermination();
	}
}