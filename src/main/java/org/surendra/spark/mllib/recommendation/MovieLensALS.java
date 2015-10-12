/**
 * 
 */
package org.surendra.spark.mllib.recommendation;

import java.util.List;
import java.util.Map;

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
public class MovieLensALS {

	private static final String BASE_PATH = "/media/surendra/Data/Source-Code/Spark Summit 2014/data/movielens/medium/";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MovieLensALS").setMaster("local[*]");
		JavaSparkContext contex = new JavaSparkContext(conf);

		JavaRDD<Rating> myRatingRdd = loadRatings(contex, "personalRatings.txt");
		JavaRDD<Rating> ratings = loadRatings(contex, "ratings.dat");
		JavaPairRDD<Integer, String> movies = loadMovieData(contex);

		/**
		 * Split ratings into train (60%), validation (20%), and test (20%) based on the last digit of the timestamp, add myRatings to train, and cache them
		 */
		int numPartitions = 4;
		
		JavaRDD<Rating> training = ratings.sample(false, 0.6).union(myRatingRdd).repartition(numPartitions).cache();
		JavaRDD<Rating> test = ratings.subtract(training);
		
		/**
		 * Evaluate model with different parameter and test data set and calculate Root Mean Squared Error.
		 * Select model with minimum RMSE
		 */
		int[] ranks = new int[]{ 8, 12 };
		double[] lambdas = new double[] { 0.01, 0.02 };
		int[] numIters = new int[] { 10, 20 };
		
		MatrixFactorizationModel bestModel = null;
		double bestValidationRmse = Double.MAX_VALUE;
		
		for (int rank : ranks) {
			for (double lambda : lambdas) {
				for (int numIter : numIters) {
					MatrixFactorizationModel model = ALS.train(training.rdd(), rank, numIter, lambda);
					double validationRmse = computeRmse(model, test);					
					if (validationRmse < bestValidationRmse) {
				        bestModel = model;
				        bestValidationRmse = validationRmse;
				      }
				}
			}			
		}
		
		/**
		 * Make personalized recommendations
		 */
	    List<Integer> myRatedMovieIds = myRatingRdd.map(rating -> rating.product()).collect();
	    JavaRDD<Integer> candidates = movies.keys().filter(key -> !myRatedMovieIds.contains(key));

	    List<Rating> recommendations = bestModel.predict(candidates.mapToPair(can ->  new Tuple2<>(0, can)))
	    		.sortBy(rating -> rating.rating(), false, 4).take(20);
	    
	    Map<Integer, String> moviesMap = movies.collectAsMap();
	    System.out.println("Movies recommended for you: ");
	    for (Rating rating : recommendations) {
			System.out.println(rating.rating() + "	-	" + moviesMap.get(rating.product()));
		}
	    
	    contex.stop();				
	}
	
	private static JavaPairRDD<Integer, String> loadMovieData(JavaSparkContext contex) {
		return contex.textFile(BASE_PATH + "movies.dat").mapToPair(line -> {
			String[] fields = line.split("::");
			return new Tuple2<Integer, String>(Integer.valueOf(fields[0].trim()), fields[1].trim());
		});
	}
	
	/** 
	 * Compute RMSE (Root Mean Squared Error)
	 */
	private static double computeRmse (MatrixFactorizationModel model, JavaRDD<Rating> test) {
		
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(
			test.mapToPair(rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product()))
		).mapToPair(rating -> new Tuple2<Tuple2<Integer, Integer>, Double>(
			new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating())
		);
	    
	    JavaRDD<Tuple2<Double, Double>> ratesAndPreds = test.mapToPair(rating ->
			new Tuple2<Tuple2<Integer, Integer>, Double>(new Tuple2<Integer, Integer>(rating.user(), rating.product()), rating.rating())
	    ).join(predictions).values();
	    
	    return Math.sqrt(ratesAndPreds.mapToDouble(tuple -> {
			Double err = tuple._1().doubleValue() - tuple._2().doubleValue();
			return err * err;
		}).mean());
	}

	private static JavaRDD<Rating> loadRatings(JavaSparkContext contex, String fileName) {
		return contex.textFile(BASE_PATH + fileName).map(line -> {
			String[] fields = line.split("::");
			return new Rating(Integer.valueOf(fields[0].trim()), Integer.valueOf(fields[1].trim()), Double.valueOf(fields[2].trim()));
		}).filter(rating -> rating.rating() > 0.0);
	}	
}
