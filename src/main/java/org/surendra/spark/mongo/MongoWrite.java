/**
 * 
 */
package org.surendra.spark.mongo;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;
import org.bson.types.ObjectId;
import org.surendra.spark.entity.User;

import scala.Tuple2;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoOutputFormat;

/**
 * Writing data in mongoDB from Spark using mongo-hadoop third party connector
 * 
 * @author surendra.singh
 *
 */
public class MongoWrite {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/**
		 * Configuration to define mongoDB properties
		 */
		Configuration mongodbConfig = new Configuration();
		
		/**
		 * Set the Output format to save the data in MongoDB
		 */
		mongodbConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
		
		/**
		 * Set mongoDB output URI to - mongodb://<host-name>:<port>/<database>.<collection-name>
		 */
		mongodbConfig.set("mongo.output.uri", "mongodb://localhost:27017/test.user");

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Mango Data write");
		JavaSparkContext context = new JavaSparkContext(conf);

		/**
		 * Create RDD of user list using SparkContext
		 */
		JavaRDD<User> userRDD = context.parallelize(getUserList());
		
		/**
		 * Create PairRDD from userRDD and set all the user data in BSON Object
		 * 
		 * ObjectId - mongoDB ObjectId
		 * BSONObject - mongoDB user document
		 */
		JavaPairRDD<ObjectId, BSONObject> bsonPairRDD = userRDD.mapToPair(user -> {
			BasicDBObject bsonObject = new BasicDBObject();
			bsonObject.append("name", user.getName());
			bsonObject.append("age", user.getAge());
			bsonObject.append("location", user.getLocation());
			return new Tuple2<ObjectId, BSONObject>(new ObjectId(), bsonObject);
		});
		
		/**
		 * Save bsonPairRDD to mongoDB using new Hadoop API
		 */
		bsonPairRDD.saveAsNewAPIHadoopFile("file:///this-is-completely-unused", ObjectId.class, BSONObject.class, MongoOutputFormat.class, mongodbConfig);	
		context.close();
	}

	private static List<User> getUserList() {
		List<User> userList = new ArrayList<User>();
		
		/**
		 * TODO
		 * 
		 * Add User objects to the userList for persist in mongoDB 
		 */

		return userList;
	}
}
