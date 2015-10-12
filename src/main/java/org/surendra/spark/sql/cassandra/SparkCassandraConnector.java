/**
 * 
 */
package org.surendra.spark.sql.cassandra;

import org.surendra.spark.sql.cassandra.job.ChannelJob;
import org.surendra.spark.sql.cassandra.job.ProductJob;


/**
 * @author surendra.singh
 *
 */
public class SparkCassandraConnector {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Thread t1 = new Thread(new ProductJob(), "Product Job");
		Thread t2 = new Thread(new ChannelJob(), "Channel Job");
		t1.run();
		t2.run();
	}
}
