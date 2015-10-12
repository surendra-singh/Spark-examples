/**
 * 
 */
package org.surendra.spark.mllib.recommendation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

/**
 * @author surendra.singh
 *
 */
public class CollaborativeFiltering {
	
	@SuppressWarnings({ "resource", "unused" })
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String path = "/home/surendra/Tools/spark-1.5.0/data/mllib/als/test.data";

		JavaRDD<Rating> ratings = sc.textFile(path).map(string -> {
			String[] sarray = string.split(",");
			return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
		});

		/**
		 * Build the recommendation model using ALS
		 */
		int rank = 10;
		int numIterations = 10;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

		/**
		 * Evaluate the model on rating data
		 */
		JavaPairRDD<Integer, Integer> userProducts = ratings.mapToPair(rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product()));

		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(userProducts).mapToPair(rating -> 
				new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating()));

		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> ratesAndPreds = ratings.mapToPair(rating ->
				new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating())
		).join(predictions);

		double MSE = Math.sqrt(ratesAndPreds.values().mapToDouble(tuple -> {
			Double err = tuple._1().doubleValue() - tuple._2().doubleValue();
			return err * err;
		}).mean());
		
		System.out.println("Mean Squared Error = " + MSE);

		/**
		 * Save and load model
		 */
		model.save(sc.sc(), "myModelPath");
		MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(sc.sc(), "myModelPath");
		
		sc.close();
	}
}
