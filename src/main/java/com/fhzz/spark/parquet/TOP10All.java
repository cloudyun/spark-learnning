package com.fhzz.spark.parquet;

import java.io.FileNotFoundException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

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
public class TOP10All {

	public static void main(String[] args) throws FileNotFoundException {
		if (args.length < 1) {
			System.err.println("Usage: TOP10All <tablename>");
			System.exit(1);
		}


		SparkConf conf = new SparkConf();
		/*
		 * 配置应用名称以及配置两个线程
		 */
		conf.setAppName("TOP10All");
		
		JavaSparkContext ctx = new JavaSparkContext(conf);
		
		//sparksql 读取parquet格式文件
		HiveContext sc = new HiveContext(ctx);
		
		DataFrame sql = sc.sql("select * from " + args[0] + " limit 10");
		
//		DataFrameReader read = sc.read();
//		
//		DataFrame table = read.table(args[0]);
		
		List<Row> take = sql.takeAsList(10);
		for (Row value : take) {
			System.out.println("clxxbh : "+ value.getAs("clxxbh"));
			System.out.println("hphm : "+ value.getAs("hphm"));
			System.out.println("puid : "+ value.getAs("puid"));
		}
		
		ctx.stop();
		ctx.close();
	}
}
