package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultQuery;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;

public class ScorerMap implements MapFunction<TokenizedNewsArticle, Byte> {

	private static final long serialVersionUID = 1L;
	private Broadcast<CorpusSummary> corpus;
	private List<Query> queryList;
	private CollectionAccumulator<DocumentRanking> queryResutsAccumulator;

	public ScorerMap(Broadcast<CorpusSummary> corpus, List<Query> queryList, CollectionAccumulator<DocumentRanking> queryResutsAccumulator) {
		this.corpus = corpus;
		this.queryList = queryList;
		this.queryResutsAccumulator = queryResutsAccumulator;
	}
	@Override
	public Byte call(TokenizedNewsArticle value) throws Exception {
		for (Query query : queryList) {
		
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
			RankedResult a = new RankedResult(value.getArticle().getId(), value.getArticle(), score/query.getQueryTerms().size());
			List<RankedResult> b = new ArrayList<>();
			b.add(a);
			DocumentRanking dr = new DocumentRanking(query, b);
			queryResutsAccumulator.add(dr);
			
		}
		return 0;
		
		
	}

}
