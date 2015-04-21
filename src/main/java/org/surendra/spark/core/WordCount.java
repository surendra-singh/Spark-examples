/**
 * 
 */
package org.surendra.spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author surendra.singh
 *
 */
public class WordCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName("Word Count");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("/home/surendra/Desktop/sample.txt");

		JavaPairRDD<String, Integer> tuple = lines.flatMap(l -> Arrays.asList(l.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((a, b) -> a + b).sortByKey(); 
		
		for (Tuple2<String, Integer> tuple2 : tuple.toArray()) {
			System.out.println(tuple2._1() + " - " + tuple2._2());
		}
		sc.close();
	}
}
