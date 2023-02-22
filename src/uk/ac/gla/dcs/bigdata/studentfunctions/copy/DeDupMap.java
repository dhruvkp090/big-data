package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

public class DeDupMap implements MapFunction<RankedResult,RankedResult> {
	
	private static final long serialVersionUID = 1L;
	private RankedResult result;

	public DeDupMap(RankedResult result) {
		this.result = result;
	}
	@Override
	public RankedResult call(RankedResult value) throws Exception {
		//here needs to be the mechanism for checking
		String title1 = result.getArticle().getTitle();
		String title2 = value.getArticle().getTitle();
		double similarity = TextDistanceCalculator.similarity(title1, title2);
		if(similarity > 0.8) {
			System.exit(0);
		}
		return result;
	}

}
