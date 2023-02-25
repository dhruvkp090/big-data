package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

/**
 * This Map function extracts the token frequency pairs of a document
 * 
 * @param value Object of type TokenizedNewsArticle
 * return TokenFrequency object
 */

public class TokenFrequencyMap implements MapFunction<TokenizedNewsArticle, TokenFrequency> {

	private static final long serialVersionUID = 23L;

	@Override
	public TokenFrequency call(TokenizedNewsArticle value) throws Exception {
		return value.getFrequency();
	}

}