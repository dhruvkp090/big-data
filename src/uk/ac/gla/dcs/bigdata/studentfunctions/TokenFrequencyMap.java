package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Map;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

/**
 * Map function that extracts the token frequency pairs of a document 
 */

public class TokenFrequencyMap implements MapFunction<TokenizedNewsArticle,Map<String,Integer>> {
	
	private static final long serialVersionUID = 23L;
	
	@Override
	public Map<String, Integer> call(TokenizedNewsArticle value) throws Exception {
		return value.getFrequency();
	}

}
