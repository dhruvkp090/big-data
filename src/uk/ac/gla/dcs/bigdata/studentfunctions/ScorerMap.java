package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class ScorerMap implements MapFunction<TokenizedNewsArticle, RankedResult> {

	private static final long serialVersionUID = 1L;
	private CorpusSummary corpus;
	private Query query;

	public ScorerMap(CorpusSummary corpus, Query query) {
		this.corpus = corpus;
		this.query = query;
	}
	@Override
	public RankedResult call(TokenizedNewsArticle value) throws Exception {
		
		
		int len = value.getLength();
		double avglen = corpus.getAverageDocumentLength();
		long docs = corpus.getTotalDocuments();
		double score = 0;
		for(String term: query.getQueryTerms()) {
			short tf = value.getFrequency().getFrequency().get(term);
			int totalfreq = corpus.getQueryTermsFrequency().getFrequency().get(term);
			double score1 = DPHScorer.getDPHScore(tf, totalfreq, len, avglen, docs);
			score += score1;
		}
		
		return new RankedResult(value.getId(), value, score);
	}

}
