package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * This merges two hashmaps of token frequency pairs, this is processed recursively for the whole dataset
 * to get sum of term frequencies for the term across all documents
 */

public class TokenFrequencyReducer implements ReduceFunction<Map<String,Integer>> {
	
	private static final long serialVersionUID = 24L;
	
	@Override
	public Map<String,Integer> call(Map<String,Integer>  m1, Map<String,Integer> m2) throws Exception {
		var v = Stream.of(m1, m2)
			    .map(Map::entrySet)
			    .flatMap(Set::stream)
			    .collect(
			        Collectors.toMap(
			            Map.Entry::getKey,   // key mapper
			            Map.Entry::getValue, // value mapper
			            Integer::sum         // merge function
			        )
			    );
		return v;
	}
	


}
