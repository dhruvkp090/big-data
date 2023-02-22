package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * This adds two integers, this is processed recursively for the whole dataset
 * to get a global sum.
 */

public class DocumentLengthReducer implements ReduceFunction<Integer> {

	private static final long serialVersionUID = 22L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		return v1+v2;
	}

}
