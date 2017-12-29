package com.fhzz.spark.kudu;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.kudu.spark.kudu.KuduContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;

/**
 * @FileName : (ReadFromKudu.java) 
 * 
 * @description  : (spark读取kudu数据)
 * @author: gaoyun
 * @version: Version No.1
 * @date: 2017年12月29日
 * @modify: 2017年12月29日 上午9:02:06
 * @copyright: FiberHome FHZ Telecommunication Technologies Co.Ltd.
 *
 */
public class ReadFromKudu {
	
	public final static String INVALID = "'-','?','无牌','00000000','XXXXXXX','11111111','0','车牌','未知','未识别','无车牌'";
	
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: ReadFromKudu <kuduMaster> <tableName> <columns>");
			System.exit(1);
		}
		
		String kuduMaster = args[0];
		
		String tableName = args[1];
		
		String[] columns = args[2].split(",");
		
		SparkConf conf = new SparkConf();
		conf.setAppName("ReadFromKudu");
		
		SparkContext sc = new SparkContext(conf);
		
		KuduContext kc = new KuduContext(kuduMaster, sc);
		
		Seq<String> seqs = JavaConverters.asScalaIterableConverter(Arrays.asList(columns)).asScala().toSeq();
		
		RDD<Row> kuduRDD = kc.kuduRDD(sc, tableName, seqs);
		ClassTag<Row> classTag = ClassManifestFactory.fromClass(Row.class);
		JavaRDD<Row> rdd = JavaRDD.fromRDD(kuduRDD, classTag);
		JavaRDD<String> map = rdd.map(new Function<Row, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return row.getString(row.schema().fieldIndex("HPHM"));
			}
		});
		
		//过滤无效数据
		JavaRDD<String> filter = map.filter(new Function<String, Boolean>() {

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
		
		JavaPairRDD<String, Integer> pair = filter.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Iterator<String> values) throws Exception {
				List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
				while (values.hasNext()) {
					String next = values.next();
					list.add(new Tuple2<String, Integer>(next, 1));
				}
				return list;
			}
		});
		
		JavaPairRDD<String, Integer> reduce = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer va, Integer vb) throws Exception {
				return va + vb;
			}
		}, 4);
		
		List<Tuple2<String, Integer>> take = reduce.take(10);
		
		for (Tuple2<String, Integer> value : take) {
			System.out.println(value._1 + ":" + value._2);
		}
		
		
		sc.stop();
	}
}
