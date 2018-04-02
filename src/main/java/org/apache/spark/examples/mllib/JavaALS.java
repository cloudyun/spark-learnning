/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * Example using MLlib ALS from Java.
 */
public final class JavaALS {

	static class ParseRating implements Function<String, Rating> {
		
		private static final long serialVersionUID = -8139210813123883480L;
		private static final Pattern COMMA = Pattern.compile(",");

		@Override
		public Rating call(String line) {
			String[] tok = COMMA.split(line);
			int x = Integer.parseInt(tok[0]);
			int y = Integer.parseInt(tok[1]);
			double rating = Double.parseDouble(tok[2]);
			return new Rating(x, y, rating);
		}
	}

	static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
		
		private static final long serialVersionUID = -5027549436361744267L;

		@Override
		public String call(Tuple2<Object, double[]> element) {
			return element._1() + "," + Arrays.toString(element._2());
		}
	}

	public static void main(String[] args) {
		String data_path = System.class.getResource("/resources").getPath() + "/data/mllib/als/test.data";
		int rank = 50;
		int iterations = 10;
		String outputDir = "E:/spark/als";
		int blocks = 1;
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaALS");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(data_path);

		JavaRDD<Rating> ratings = lines.map(new ParseRating());

		MatrixFactorizationModel model = ALS.train(ratings.rdd(), rank, iterations, 0.01, blocks);

		model.userFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(outputDir + "/userFeatures");
		model.productFeatures().toJavaRDD().map(new FeaturesToString()).saveAsTextFile(outputDir + "/productFeatures");
		System.out.println("Final user/product features written to " + outputDir);

		sc.stop();
		sc.close();
	}
}
