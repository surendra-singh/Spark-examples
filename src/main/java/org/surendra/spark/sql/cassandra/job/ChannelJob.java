package org.surendra.spark.sql.cassandra.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.surendra.spark.sql.cassandra.entity.Channel;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.writer.RowWriterFactory;

/**
 * 
 * @author surendra.singh
 *
 */
public class ChannelJob implements Runnable {

	@Override
	@SuppressWarnings("resource")
	public void run() {
		SparkConf conf = new SparkConf().setAppName("Spark-Cassandra-Channel").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
				.set("spark.cassandra.auth.username", "cassandra").set("spark.cassandra.auth.password", "cassandra");

		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<Channel> stringRDD = context.textFile("/media/surendra/Data/test-data/csv/Jan15Report.csv").map(row -> {
			String[] datArr = row.split(",");
			Channel channel = new Channel();
			channel.setOrdnr(datArr[0]);
			channel.setDate(datArr[1]);
			channel.setChannel1(datArr[2]);
			channel.setChannel2(datArr[3]);
			channel.setSource(datArr[4]);
			return channel;
		});

		RowWriterFactory<Channel> factory = CassandraJavaUtil.mapToRow(Channel.class, Channel.getFieldMap());
		CassandraJavaUtil.javaFunctions(stringRDD).writerBuilder("fame", "channel", factory).saveToCassandra();
		context.close();
	}
}
