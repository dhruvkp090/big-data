package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;

/**
 * This merges two hashmaps of token frequency pairs, this is processed recursively for the whole dataset
 * to get sum of term frequencies for the term across all documents
 */

public class TokenFrequencyReducer implements ReduceFunction<TokenFrequency> {
	
	private static final long serialVersionUID = 24L;
	
	@Override
	public TokenFrequency call(TokenFrequency  tf1, TokenFrequency tf2) throws Exception {
		Map<String, Integer> m1 = tf1.getFrequency();
		Map<String, Integer> m2 = tf2.getFrequency();
		var m = Stream.of(m1, m2)
			    .map(Map::entrySet)
			    .flatMap(Set::stream)
			    .collect(
			        Collectors.toMap(
			            Map.Entry::getKey,   // key mapper
			            Map.Entry::getValue, // value mapper
			            Integer::sum         // merge function
			        )
			    );
		HashMap<String, Integer> hm = (HashMap<String, Integer>)m;
		TokenFrequency tf_all = new TokenFrequency(hm);
		return tf_all;
	}
	


}
