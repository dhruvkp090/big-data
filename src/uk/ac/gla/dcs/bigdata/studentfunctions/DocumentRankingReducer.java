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
				int index = checkSimilarity(dr2_results.get(j),dr1_results);
				if (index == -2) {
					if (dr1_results.size() >= 10) {
						dr1_results.set(lastIndex,dr2_results.get(j));
						
					}else {
						dr1_results.add(dr2_results.get(j));
					}
					
					Collections.sort(dr1_results, Collections.reverseOrder());
				}else {
					
					if (index != -1) {
						dr1_results.set(index,dr2_results.get(j));
					}
				}
				
			}
		}
		
		
		return dr1;
	}
	/**
	 * Helper function that checks whether object of type RankedResult is similar to any of the objects
	 * in list of RankedResult objects. Objects are being compared according to the title of the Article they contain.
	 * If current is similar to one object in the all list then compare if the one object in the list has smaller score
	 * if yes then replace it, if it is similar to more than 1 in the list it is disregarded.
	 * 
	 * @param current Element of type RankedResult
	 * @param all 	  List of RankedResult elements
	 * @return		  -2 if not similar, -1 if similar to more than 1, otherwise index of the one it is similar to and bigger than
	 */	
	private static int checkSimilarity(RankedResult current, List<RankedResult>all) {
		int index = -2;
		for(int i=0; i<all.size(); i++) {
			if(TextDistanceCalculator.similarity(current.getArticle().getTitle(),all.get(i).getArticle().getTitle())<0.5){
				if(current.getScore() > all.get(i).getScore()) {
					if (index == -2) {
						index = i;
					}else {
						return -1;
					}
					
				}
			}
		}
		return index;
	}

}
