/**
 * 
 */
package org.surendra.spark.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @author surendra.singh
 *
 */
public class HiveDataLoad {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Hive Data Fetch").setMaster("local[*]");

		SparkContext context = new SparkContext(conf);
		HiveContext hiveContext = new HiveContext(context);
		DataFrame df = hiveContext.sql("Select * from hive.swiss");
		
		System.out.println("***************************************************" + df.count());
		df.toJavaRDD().foreach(System.out::println);
	}
}
