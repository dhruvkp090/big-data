package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

public class DocumentRankingReducer implements ReduceFunction<DocumentRanking> {

	private static final long serialVersionUID = 1L;

	@Override
	public DocumentRanking call(DocumentRanking v1, DocumentRanking v2) throws Exception {
		List<RankedResult> v1_results = v1.getResults();
		List<RankedResult> v2_results = v2.getResults();
		int lastIndex =  v1_results.size()-1;
		for(int j=0; j<v2_results.size();j++) {
			if (v2_results.get(j).getScore()> v1_results.get(lastIndex).getScore()) {
				if (!CheckSimilarity(v2_results.get(j),v1_results)) {
					if (v1_results.size() >= 10) {
						v1_results.set(lastIndex,v2_results.get(j));
						
					}else {
						v1_results.add(v2_results.get(j));
					}
					
					Collections.sort(v1_results, Collections.reverseOrder());
				}
				
			}
		}
		
		
		return v1;
	}
	
	private static boolean CheckSimilarity(RankedResult current, List<RankedResult>all) {
		for(RankedResult r:all) {
			if (TextDistanceCalculator.similarity(r.getArticle().getTitle() , current.getArticle().getTitle())<0.5){
				return true;
			}
		}
		return false;
	}
}
