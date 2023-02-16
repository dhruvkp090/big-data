package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class ScorerMap implements MapFunction<TokenizedNewsArticle, RankedResult> {

	private static final long serialVersionUID = 1L;
	private Broadcast<CorpusSummary> corpus;
	private Query query;

	public ScorerMap(Broadcast<CorpusSummary> corpus, Query query) {
		this.corpus = corpus;
		this.query = query;
	}
	@Override
	public RankedResult call(TokenizedNewsArticle value) throws Exception {
		
		
		int len = value.getLength();
		double avglen = corpus.value().getAverageDocumentLength();
		long docs = corpus.value().getTotalDocuments();
		double score = 0;
		short tf = 1;
		int totalfreq = 1;
		for(String term: query.getQueryTerms()) {
			if(value.getFrequency().getFrequency().get(term) != null) {
				tf = value.getFrequency().getFrequency().get(term);
				}
			if(corpus.value().getQueryTermsFrequency().getFrequency().get(term) !=null) {
				totalfreq = corpus.value().getQueryTermsFrequency().getFrequency().get(term);
			}
			double score1 = DPHScorer.getDPHScore(tf, totalfreq, len, avglen, docs);
			score += score1;
		}
		
		return new RankedResult(value.getId(), value, score);
	}

}
