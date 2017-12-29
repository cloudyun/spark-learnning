package com.fhzz.spark.kudu;

import java.util.Arrays;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class Test {
	
	public static void main(String[] args) {
//		org.apache.spark.util.AccumulatorV2 v2;
		String[] cloumns = new String[] {
				"a", "b"
		};
		
		Seq<String> seq = JavaConverters.asScalaIterableConverter(Arrays.asList(cloumns)).asScala().toSeq();
		
		System.out.println(seq.toList());
	}
}
