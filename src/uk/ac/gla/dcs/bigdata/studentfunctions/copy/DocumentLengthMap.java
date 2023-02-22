package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

/**
 * Map function that extracts the length of a document
 */

public class DocumentLengthMap implements MapFunction<TokenizedNewsArticle,Integer> {
	
	private static final long serialVersionUID = 21L;
	
	@Override
	public Integer call(TokenizedNewsArticle value) throws Exception {
		return value.getLength();
	}

}
