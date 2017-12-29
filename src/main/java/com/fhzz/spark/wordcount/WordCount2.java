package com.fhzz.spark.wordcount;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount2 {
	
	public static void main(String[] args) throws FileNotFoundException {
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}
		
		// 设置匹配模式，以空格分隔
		final Pattern SPACE = Pattern.compile(" ");


		SparkConf conf = new SparkConf();
		/*
		 * 配置应用名称以及配置两个线程
		 */
		conf.setAppName("WordCountOnline")
//		.setMaster("yarn-client")
		;
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);
		
		lines.repartition(2);
		
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String value) throws Exception {
				return Arrays.asList(SPACE.split(value));
			}
		});
		
		JavaRDD<String> filter = words.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String key) throws Exception {
				if ("".equals(key.trim()) || key.matches("\\d+")) {
					return false;
				} else {
					return true;
				}
			}
		});
		
		JavaPairRDD<String, Integer> ones = filter.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		});
		
		
		JavaPairRDD<String, Integer> sort = counts.sortByKey(new MyComparator());
//		JavaPairRDD<String, Integer> sort = counts.sortByKey();
		
		
		
		List<Tuple2<String, Integer>> output = sort.collect();
		
		for (Tuple2<String, Integer> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		
		ctx.stop();
		ctx.close();
	}
	
	private static class MyComparator implements Comparator<String>, Serializable {
		
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(String o1, String o2) {
			String[] o1s = o1.split(" ");
			String[] o2s = o2.split(" ");
			if (o1s[0].compareTo(o2s[0]) == 0)
				return o1s[1].compareTo(o2s[1]);
			else
				return -o1s[0].compareTo(o2s[0]);
		}
	}
}
