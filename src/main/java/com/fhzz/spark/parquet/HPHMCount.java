package com.fhzz.spark.parquet;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class HPHMCount {

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}


		SparkConf conf = new SparkConf();
		/*
		 * 配置应用名称以及配置两个线程
		 */
		conf.setAppName("HPHMCountFromParquet");
		
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		SQLContext sc = new SQLContext(ctx);
		
		DataFrame df = sc.read().load(args[0]).select("hphm");
		
		JavaRDD<Row> rows = df.javaRDD();
		
		JavaRDD<String> hphms = rows.flatMap(new FlatMapFunction<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Row row) throws Exception {
				return Arrays.asList(new String((byte[]) row.get(0)));
			}
		});
		
		JavaRDD<String> filter = hphms.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String key) throws Exception {
				if ("".equals(key.trim())) {
					return false;
				} else {
					return true;
				}
			}
		});
		
		JavaPairRDD<String, Integer> ones = filter.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String hphm) throws Exception {
				return new Tuple2<String, Integer>(hphm, 1);
			}
		});
		
		
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		});
		
		JavaPairRDD<String, Integer> sort = counts.sortByKey(new MyComparator());
		
		List<Tuple2<String, Integer>> output = sort.take(10);
		
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
			if (o1s[0].compareTo(o2s[0]) == 0) {
				return o1s[1].compareTo(o2s[1]);
			} else {
				return -o1s[0].compareTo(o2s[0]);
			}
		}
	}
}
