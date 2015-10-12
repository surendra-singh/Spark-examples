/**
 *
 */
package org.surendra.spark.mllib;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/**
 * Utility class to find of POS in the given text
 * 
 * @author surendra.singh
 *
 */
public class POSFinder {

	private static POSFinder posFinder;
	
	private POSModel posModel;
	
	private TokenizerModel tockenModel;
	
	/**
	 * @return
	 */
	public static POSFinder getInstance() {
		if (posFinder == null) {
			posFinder = new POSFinder();
		}
		return posFinder;
	}
	
	/**
	 * Create Model for POS finder and String tokenizer 
	 */
	private POSFinder () {
		try {
			InputStream modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream("en-pos-maxent.bin");
			this.posModel = new POSModel(modelIn);
			modelIn.close();
			
			modelIn = Thread.currentThread().getContextClassLoader().getResourceAsStream("en-token.bin");
			this.tockenModel = new TokenizerModel(modelIn);
			modelIn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Return map of POS and their count in the given string
	 * 
	 * @param data
	 * @param posType
	 * @return
	 */
	public Map<String, Integer> POSMap(String data, List<String> posType) {
		String tocken[] = getTockenizedData(data);
		Map<String, Integer> posMap = new HashMap<String, Integer>();
		try {
			String tags[] = new POSTaggerME(this.posModel).tag(tocken);
			for (int i = 0; i < tocken.length; i++) {
				if(posType.contains(tags[i])) {
					if (posMap.get(tocken[i]) == null) {
						posMap.put(tocken[i], 1);
					} else {
						posMap.put(tocken[i], posMap.get(tocken[i]) + 1);
					}				
				}
			}
		} catch (Exception e) {
			System.out.println("*****************************************************************");
			System.out.println(data);
			throw e;
		}
		return posMap;
	}

	/**
	 * Tockenize given string
	 * @param data
	 * @return
	 */
	private String[] getTockenizedData(String data) {
		return new TokenizerME(this.tockenModel).tokenize(data);
	}
}
