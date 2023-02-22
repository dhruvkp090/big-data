package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultQuery;

public class getQueryfromRRQ implements MapFunction<RankedResultQuery, Query> {
	private static final long serialVersionUID = 1L;

	@Override
	public Query call(RankedResultQuery value) throws Exception {
		return value.getQuery();
	}



}
