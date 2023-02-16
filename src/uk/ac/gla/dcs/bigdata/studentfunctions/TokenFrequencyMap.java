package uk.ac.gla.dcs.bigdata.studentfunctions;



import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

/**
 * Map function that extracts the token frequency pairs of a document 
 */

public class TokenFrequencyMap implements MapFunction<TokenizedNewsArticle,TokenFrequency> {
	
	private static final long serialVersionUID = 23L;
	
	@Override
	public TokenFrequency call(TokenizedNewsArticle value) throws Exception {
		return value.getFrequency();
	}

}
