/**
 * 
 */
package org.surendra.spark.mllib.clustering;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.hive.HiveContext;

import scala.Tuple2;

/**
 * Clustering is an un-supervised learning
 * 
 * 1)	Vectorize your data
 * 2)	Choose K center points (centroids)
 * 3)	Assign each vector to the group that has the closest centroid
 * 4)	Recalculate the positions of the centroids
 * 5)	Repeat the third and fourth steps until the centroids are not moving (the iterative stuff).
 * 
 * @author surendra.singh
 *
 */
public class Kmean {

	private static final String PATH = "/home/surendra/Tools/spark-1.5.0/data/mllib/kmeans_data.txt";

	private static final int NUM_CLUSTER = 4;
	
	private static final int TOTAL_ITERATIONS = 20;
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("K-means Example").setMaster("local[*]");
		
		JavaRDD<Vector> parsedData = loadHiveData(conf);		
		parsedData.cache();
		
		KMeansModel clusters = KMeans.train(parsedData.rdd(), NUM_CLUSTER, TOTAL_ITERATIONS);
		System.out.println("Cluster centers: ");
		for (Vector vector : clusters.clusterCenters()) {
			System.out.println(vector);
		}
	    
	    parsedData.map(data -> new Tuple2<>(data, clusters.predict(data))).foreach(System.out::println);
	    System.out.println("Compute Cost - " + clusters.computeCost(parsedData.rdd()));
	}
	
	/**
	 * @param conf
	 * @return
	 */
	private static JavaRDD<Vector> loadHiveData(SparkConf conf) {
		SparkContext context = new SparkContext(conf);
		HiveContext hiveContext = new HiveContext(context);
		
		JavaRDD<Vector> parsedData = hiveContext.sql("Select catholic from hive.swiss").javaRDD().map(row -> {
			double[] values = new double[1];
			values[0] = row.getDecimal(0).doubleValue();
			return Vectors.dense(values);
		});
		
		return parsedData;
	}
	
	/**
	 * @param conf
	 * @return
	 */
	@SuppressWarnings({ "resource", "unused" })
	private static JavaRDD<Vector> loadFileData(SparkConf conf) {
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Vector> parsedData = sc.textFile(PATH).map(row -> { 
			String[] sarray = row.split(" ");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length; i++) {
				values[i] = Double.valueOf(sarray[i]);
			}
			return Vectors.dense(values);
		});
		return parsedData;
	}
}
