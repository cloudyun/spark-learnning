package com.fhzz.spark.parquet;

import java.io.FileNotFoundException;
import java.util.Arrays;
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

/**
 * @FileName : (HPHMTOP10.java) 
 * 
 * @description  : (出现频率top10)
 * @author: gaoyun
 * @version: Version No.1
 * @date: 2017年12月13日
 * @modify: 2017年12月13日 下午2:33:27
 * @copyright: FiberHome FHZ Telecommunication Technologies Co.Ltd.
 *
 */
public class HPHMTOP10 {

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}


		SparkConf conf = new SparkConf();
		/*
		 * 配置应用名称以及配置两个线程
		 */
		conf.setAppName("HPHMTOP10");
		
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		
		//sparksql 读取parquet格式文件
		SQLContext sc = new SQLContext(ctx);
		DataFrame df = sc.read().load(args[0]).select("hphm");
		
		//DataFrame -> RDD
		JavaRDD<Row> rows = df.javaRDD();
		
		//Row 提取hphm
		JavaRDD<String> hphms = rows.flatMap(new FlatMapFunction<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Row row) throws Exception {
				return Arrays.asList(new String((byte[]) row.get(0)));
			}
		});
		
		//过滤无效数据
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
		
		//计数
		JavaPairRDD<String, Integer> ones = filter.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String hphm) throws Exception {
				return new Tuple2<String, Integer>(hphm, 1);
			}
		});
		
		//每个阶段合并相同的key
		JavaPairRDD<String, Integer> reduceByKey = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		//反转 key 和 value
		JavaPairRDD<Integer, String> mapToPair = reduceByKey.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> value) throws Exception {
				return new Tuple2<Integer, String>(value._2, value._1);
			}
		});
		
		//排序
		JavaPairRDD<Integer, String> sortByKey = mapToPair.sortByKey(false);
		
		//再次反转
		JavaPairRDD<String, Integer> mapToPair2 = sortByKey.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> value) throws Exception {
				return new Tuple2<String, Integer>(value._2, value._1);
			}
		});
		
		//取前10条数据
		List<Tuple2<String, Integer>> take = mapToPair2.take(10);
		for (Tuple2<String, Integer> value : take) {
			System.out.println("hphm : "+ value._1 + "... count : "+value._2);
		}
		
		ctx.stop();
		ctx.close();
	}
}
