package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

public class RankedResultToScore implements MapFunction<RankedResult,Double> {
	
	private static final long serialVersionUID = 1L;
	
	@Override
	public Double call(RankedResult value) throws Exception {
		return value.getScore();
	}
}
