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
		/**
		 * Create SparkConfig Object and set master URL and Application Name
		 */
		SparkConf sparkConf = new SparkConf();
		sparkConf.setMaster("local[*]");
		sparkConf.setAppName("Word Count");
		
		/**
		 * Create JavaSparkContex Object, passing SparkConfig as argument  
		 */
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		/**
		 * Load sample data as text file. Spark will return RDD of String.
		 * Each line in file represent one record in RDD
		 */
		JavaRDD<String> lines = sc.textFile("/home/surendra/Desktop/sample.txt");

		/**
		 * Word count after, map and reduce occurrence of each word
		 */
		JavaPairRDD<String, Integer> tuple = lines.flatMap(l -> Arrays.asList(l.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey((a, b) -> a + b).sortByKey(); 
		
		/**
		 * Collect the Tuples of word and their count and print
		 */
		for (Tuple2<String, Integer> tuple2 : tuple.collect()) {
			System.out.println(tuple2._1() + " - " + tuple2._2());
		}
		sc.close();
	}
}
