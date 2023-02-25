package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;


/**
 * Flatmap which filters the documents without title or content and 
 * calculate the term frequecies of the documents for terms in the queries
 * 
 * @param news NewsArticle 	object
 * @param queryTerms 		terms present in the queries
 * @param totalDocLength 	Accumulator which calculates the total document length
 * @return docTermFreq 		TokenizedNewsArticle object 
 */

public class NewsTokenizerFlatMap implements FlatMapFunction<NewsArticle, TokenizedNewsArticle> {
	
	
	private static final long serialVersionUID = 1L;
	private Map<String, Integer> queryTerms;
	private LongAccumulator totalDocLength;
	
	public NewsTokenizerFlatMap(Map<String, Integer> queryTerms,LongAccumulator totalDocLength) {
		this.queryTerms = queryTerms;	
		this.totalDocLength = totalDocLength;
	}
	
	@Override
	public Iterator<TokenizedNewsArticle> call(NewsArticle news) throws Exception {
		/* Checking if either the title or content is null */
		if (news.getTitle()!= null && news.getContents()!=null) {
			TextPreProcessor tokenize = new TextPreProcessor();
			/* Tokenize the title */
        	List<String> tokenizedTitle = tokenize.process(news.getTitle());

        	/* Merge paragraphs and get only the first five paragraphs if there are more*/
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
        	/* Tokenize the paragraphs */
        	List<String> docTerms = tokenize.process(firstFivePara); 


            HashMap<String, Integer> frequency = new HashMap<>();

            /* For each term in the queries gets the number of times term appear in the document*/
            for (String token : queryTerms.keySet()) {
            	int occurrences = Collections.frequency(docTerms, token);
            	frequency.put(token, occurrences);
				queryTerms.put(token, queryTerms.get(token) + occurrences);
            }
            /* Creates the TokenFrequency object*/
            TokenFrequency frequency_object = new TokenFrequency(frequency);
            
            /* Adds the document length to the accumulator*/
            totalDocLength.add(docTerms.size());
            
            List<TokenizedNewsArticle> docTermFreq = new ArrayList<>(1);
            docTermFreq.add(new TokenizedNewsArticle(tokenizedTitle,docTerms.size()+1,frequency_object,news));
            return docTermFreq.iterator();

        	
		}else {
			/* Returning an empty list if either the title or content is null */
            List<TokenizedNewsArticle> docTermFreq = new ArrayList<>(0);
            return docTermFreq.iterator();
		}
	}

}
