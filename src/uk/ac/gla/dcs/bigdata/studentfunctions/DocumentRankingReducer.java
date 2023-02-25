package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
/**
 * This reducer reduces 2 objects of type DocumentRanking into 1. 
 * For every pair of DocumentRankings, assume that these are sorted and
 * take the biggest element of dr2 and compare with the smallest element of dr1
 * if true, it checks whether the element from dr2 is similar to any of the elements in dr1, if true(i.e. not similar)
 * then the biggest element of dr2 replaces the smallest element of dr1 in dr1 and dr1 is sorted.
 * Then we move on to compare the currently smallest element of dr1 with the currently biggest element of dr2.
 * 
 * @param dr1  Object of type DocumentRanking
 * @param dr2  Object of type DocumentRanking
 * @return     dr1, either modified or not modified
 */
public class DocumentRankingReducer implements ReduceFunction<DocumentRanking> {

	private static final long serialVersionUID = 1L;

	@Override
	public DocumentRanking call(DocumentRanking dr1, DocumentRanking dr2) throws Exception {
		List<RankedResult> dr1_results = dr1.getResults();
		List<RankedResult> dr2_results = dr2.getResults();
		int lastIndex =  dr1_results.size()-1;
		for(int j=0; j<dr2_results.size();j++) {
			if (dr2_results.get(j).getScore()> dr1_results.get(lastIndex).getScore()) {
				if (!CheckSimilarity(dr2_results.get(j),dr1_results)) {
					if (dr1_results.size() >= 10) {
						dr1_results.set(lastIndex,dr2_results.get(j));
						
					}else {
						dr1_results.add(dr2_results.get(j));
					}
					
					Collections.sort(dr1_results, Collections.reverseOrder());
				}
				
			}
		}
		
		
		return dr1;
	}
	/**
	 * Helper function that checks whether object of type RankedResult is similar to any of the objects
	 * in list of RankedResult objects. Objects are being compared according to the title of the Article they contain.
	 * 
	 * @param current Element of type RankedResult
	 * @param all 	  List of RankedResult elements
	 * @return		  True if similar, false if not similar
	 */
	private static boolean CheckSimilarity(RankedResult current, List<RankedResult>all) {
		for(RankedResult r:all) {
			if (TextDistanceCalculator.similarity(r.getArticle().getTitle() , current.getArticle().getTitle())<0.5){
				return true;
			}
		}
		return false;
	}
}
