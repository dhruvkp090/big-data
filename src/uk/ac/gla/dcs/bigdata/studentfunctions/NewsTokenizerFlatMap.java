package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

public class NewsTokenizerFlatMap implements FlatMapFunction<NewsArticle, TokenizedNewsArticle> {
	
	
	private static final long serialVersionUID = 1L;
	private List<String> queryTerms;
	private LongAccumulator totalDocLength;
	
	public NewsTokenizerFlatMap(List<String> queryTerms,LongAccumulator totalDocLength) {
		this.queryTerms = queryTerms;	
		this.totalDocLength = totalDocLength;
	}
	
	@Override
	public Iterator<TokenizedNewsArticle> call(NewsArticle news) throws Exception {
		if (news.getTitle()!= null && news.getContents()!=null) {
			TextPreProcessor tokenize = new TextPreProcessor();
        	List<String> tokenizedTitle = tokenize.process(news.getTitle());

        	/* Doc Term Frequency */
        	List<ContentItem> contents = news.getContents();
        	int count = 0;
        	String firstFivePara = "";
        	for (ContentItem content : contents) {
        		if (content != null && content.getSubtype() != null && content.getSubtype().equals("paragraph")) {
        			firstFivePara = firstFivePara + " " + content.getContent().replaceAll("http.*?\\s", " ");
        			count++;
        		}
        		if (count >= 5) {
                break;
        		}
        	}
        	List<String> docTerms = tokenize.process(firstFivePara); // Tokenize Docterms
        	docTerms.addAll(tokenizedTitle);
        	
            HashMap<String, Integer> frequency = new HashMap<>();

            // For each term in the queries list gets the number of times term appear in the document
            for (String token : queryTerms) {
            	int occurrences = Collections.frequency(docTerms, token);
            	frequency.put(token, occurrences+1);
            }
            TokenFrequency frequency_object = new TokenFrequency(frequency);
            totalDocLength.add(docTerms.size());
            List<TokenizedNewsArticle> docTermFreq = new ArrayList<>(1);
            
            docTermFreq.add(new TokenizedNewsArticle(tokenizedTitle,docTerms.size(),frequency_object,news));
            return docTermFreq.iterator();

        	
		}else {
            List<TokenizedNewsArticle> docTermFreq = new ArrayList<>(0);
            return docTermFreq.iterator();
		}
	}

}
