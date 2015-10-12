/**
 * 
 */
package org.surendra.spark.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author surendra.singh
 *
 */
public class BasicStatistics {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Basic Stats").setMaster("local[*]");
		SparkContext context = new SparkContext(conf);
		
		HiveContext hiveContext = new HiveContext(context);
		DataFrame swissDF = hiveContext.sql("Select * from hive.swiss");
		
		swissDF.javaRDD().foreach(System.out::println);
	}
}
