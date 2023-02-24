package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

public class KeyFunctionMap implements MapFunction<DocumentRanking, Query> {

	private static final long serialVersionUID = 1L;

	@Override
	public Query call(DocumentRanking value) throws Exception {
		return value.getQuery();
	}
	
}
