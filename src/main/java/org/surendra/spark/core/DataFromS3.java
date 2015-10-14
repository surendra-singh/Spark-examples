/**
 * 
 */
package org.surendra.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author surendra.singh
 *
 */
public class DataFromS3 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		final JavaSparkContext sparkContext = new JavaSparkContext("local[*]", "Data From S3", new SparkConf());
		sparkContext.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ".....");
		sparkContext.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "....");
		
		final JavaRDD<String> lines = sparkContext.textFile("s3n://<bucket_name>/<file_path>");
		final Tuple2<Integer, Integer> tuple = lines.mapToPair(string -> new Tuple2<>(string.length(), 1))
				.reduceByKey((a, b) -> a + b).sortByKey(false).first();
		
		System.out.println("Length - " + tuple._1());
		System.out.println("Count - " + tuple._2());
		
		sparkContext.close();
	}
}
