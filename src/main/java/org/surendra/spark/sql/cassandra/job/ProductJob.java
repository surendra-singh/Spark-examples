/**
 * 
 */
package org.surendra.spark.sql.cassandra.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.surendra.spark.sql.cassandra.entity.Product;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.writer.RowWriterFactory;

/**
 * @author surendra.singh
 *
 */
public class ProductJob implements Runnable {

	@Override
	@SuppressWarnings("resource")
	public void run() {
		SparkConf conf = new SparkConf().setAppName("Spark-Cassandra-Channel").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
				.set("spark.cassandra.auth.username", "cassandra").set("spark.cassandra.auth.password", "cassandra");
		
		JavaSparkContext context = new JavaSparkContext(conf);		
		JavaRDD<Product> stringRDD = context.textFile("/media/surendra/Data/test-data/csv/Master-Product.csv").map(row -> {
			String[] datArr = row.split(",");
			Product product = new Product();
			product.setId(Long.valueOf(datArr[0]));
			product.setOrdnr(datArr[1]);
			product.setClasss(datArr[2]);
			product.setVendor(datArr[3]);
			return product;
		});
		
		RowWriterFactory<Product> factory = CassandraJavaUtil.mapToRow(Product.class, Product.getFieldMap());		
		CassandraJavaUtil.javaFunctions(stringRDD).writerBuilder("fame", "product", factory).saveToCassandra();
		context.close();
	}
}
