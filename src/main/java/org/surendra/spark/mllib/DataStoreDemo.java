/**
 * 
 */
package org.surendra.spark.mllib;

import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.regression.LabeledPoint;


/**
 * @author surendra.singh
 *
 */
public class DataStoreDemo {

	/**
	 * @param args
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {

		Vectors.dense(new double[] {});

		Vectors.sparse(3, new int[] { 1, 2 }, new double[] { 5, 6 });

		new LabeledPoint(0, Vectors.sparse(10, new int[] { 3, 6 }, new double[] { 8, 3 }));

		Matrix dene = Matrices.dense(3, 2, new double[] { 1, 2, 3, 4, 5, 6 });

		Matrix sm = Matrices.sparse(4, 3, new int[] { 0, 1, 3, 4 }, new int[] { 0, 1, 2, 3 }, new double[] { 9, 8, 6, 4 });

		IndexedRow row = new IndexedRow(0, Vectors.sparse(3, new int[] { 0, 1 }, new double[] { 6, 9 }));
		
	}
}
