package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
/**
 * This MapFunction calculates DPH score for a given TokenizedNewsArticle object wrt. query
 * Score is calculated for each term in the query, which are then summed and divided by
 * the number of terms in the query. This score is used to create a RankedResult object
 * that is then used to create a DocumentRanking object that also contains the query.
 * 
 * @param corpus Object of type CorpusSummary, containing information about the collection
 * @param query  Query with respect to which the document needs to be ranked
 * @param value  TokenizedNewsArticle object that is to be ranked
 * @return 		 New DocumentRanking object
 */
public class ScorerMap implements MapFunction<TokenizedNewsArticle, DocumentRanking> {

	private static final long serialVersionUID = 1L;
	private Broadcast<CorpusSummary> corpus;
	private Query query;
	
	public ScorerMap(Broadcast<CorpusSummary> corpus, Query query) {
		this.corpus = corpus;
		this.query = query;
	}
	@Override
	public DocumentRanking call(TokenizedNewsArticle value) throws Exception {
		
		
		int len = value.getLength();
		double avglen = corpus.value().getAverageDocumentLength();
		long docs = corpus.value().getTotalDocuments();
		double score = 0;
		for(String term: query.getQueryTerms()) {
			short tf = 1;
			int totalfreq = 1;
			if(value.getFrequency().getFrequency().get(term) != null) {
				tf = value.getFrequency().getFrequency().get(term).shortValue();
				}
			if(corpus.value().getQueryTermsFrequency().getFrequency().get(term) !=null) {
				totalfreq = corpus.value().getQueryTermsFrequency().getFrequency().get(term);
			}
			double score1 = DPHScorer.getDPHScore((short)(tf+1), totalfreq+1, len, avglen, docs);
			score += score1;
		}
		
		RankedResult r =  new RankedResult(value.getArticle().getId(), value.getArticle(), score/(query.getQueryTerms().size()));
		List<RankedResult> l = new ArrayList<>();
		l.add(r);
		return new DocumentRanking(query, l);
	}

}
