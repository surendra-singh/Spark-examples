/**
 * 
 */
package org.surendra.spark.mllib.classification;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Logistic regression is widely used to predict a binary response
 * 
 * @author surendra.singh
 *
 */
public class ClassificationDemo {

	/**
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Naive Bayes Example").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> spam = jsc.textFile("spam.txt");
		JavaRDD<String> normal = jsc.textFile("normal.txt");
		
		// Create a HashingTF instance to map email text to vectors of 10,000 features.
		final HashingTF tf = new HashingTF(10000);
		
		// Create LabeledPoint data sets for positive (spam) and negative (normal) examples.
		JavaRDD<LabeledPoint> posExamples = spam.map(string -> new LabeledPoint(1, tf.transform(Arrays.asList(string.split(" ")))));
		JavaRDD<LabeledPoint> negExamples = normal.map(string -> new LabeledPoint(0, tf.transform(Arrays.asList(string.split(" ")))));
		
		JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
		
		//Cache since Logistic Regression is an iterative algorithm.
		trainData.cache(); 
		
		logisticRegression(trainData, tf);
	}
	
	/**
	 * @param trainData
	 * @param tf
	 */
	private static void logisticRegression(JavaRDD<LabeledPoint> trainData, HashingTF tf) {
		// Run Logistic Regression using the SGD algorithm.
		LogisticRegressionModel model = new LogisticRegressionWithSGD().run(trainData.rdd());
		
		// Test on a positive example (spam) and a negative one (normal).
		Vector posTest = tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
		Vector negTest = tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));
		
		System.out.println("Prediction for positive example: " + model.predict(posTest));
		System.out.println("Prediction for negative example: " + model.predict(negTest));
	}
}
