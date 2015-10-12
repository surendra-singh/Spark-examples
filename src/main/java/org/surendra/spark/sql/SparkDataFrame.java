/**
 * 
 */
package org.surendra.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author surendra.singh
 *
 */
public class SparkDataFrame {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setMaster("local[*]").setAppName("Data Frame");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		DataFrame df = sqlContext.read().jdbc("jdbc:mysql://localhost/test?user=root", "user", null);
		df.select("name").show();
		
		sparkContext.close();
	}
}
