/**
 * 
 */
package org.surendra.spark.mongo;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import org.surendra.spark.entity.User;

import com.mongodb.hadoop.MongoInputFormat;

/**
 * Reading data in Spark from mongoDb using mongo-hadoop third party connector
 * 
 * @author surendra.singh
 *
 */
public class MongoRead {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/**
		 * Configuration to define mongoDB properties
		 */
		Configuration mongodbConfig = new Configuration();
		
		/**
		 * Set the Input format to load the data
		 */
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		
		/**
		 * Set mongoDB URI for - mongodb://<host-name>:<port>/<database>.<collection-name>
		 */
		mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/test.user");

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Mango Data Read");
		JavaSparkContext context = new JavaSparkContext(conf);

		/**
		 * Read the data from mongoDb using SparkContext object, it returns JavaPairRDD
		 * Object - mongoDB ObjectId
		 * BSONOBJECT - mongoDB document
		 */
		JavaPairRDD<Object, BSONObject> documents = context.newAPIHadoopRDD(
				mongodbConfig, MongoInputFormat.class, Object.class, BSONObject.class);

		/**
		 * Map loaded BSONObject to custom objects
		 */
		@SuppressWarnings("resource")
		JavaRDD<User> userRDD = documents.map(t -> {
			User user = new User();
			user.setName(t._2().get("name").toString());
			user.setLocation(t._2().get("location").toString());
			user.setAge((int) t._2().get("age"));
			return user;
		});

		/**
		 * Print user information fetched from mongoDb
		 */
		List<User> userList = userRDD.collect();
		for (User user : userList) {
			System.out.println("**************************************************");
			System.out.println("User Name - " + user.getName());
			System.out.println("User Age - " + user.getAge());
			System.out.println("User Location - " + user.getLocation());
		}
		
		context.close();
	}
}
