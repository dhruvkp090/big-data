package uk.ac.gla.dcs.bigdata.studentfunctions;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;

import java.util.HashMap;
import java.util.List;

public class NewsTokenizerMap implements MapFunction<NewsArticle, TokenizedNewsArticle> {

    private static final long serialVersionUID = 1L;
	private SparkSession spark;


    public NewsTokenizerMap(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public TokenizedNewsArticle call(NewsArticle news) throws Exception {
        /*     Title Tokenizing     */
        TextPreProcessor tokenize = new TextPreProcessor();
        List<String> tokenizedTitle = tokenize.process(news.getTitle());

        /*  Doc Term Frequency  */
        List<ContentItem> contents = news.getContents();
        int count = 0;
        String firstFivePara = "";
        for(ContentItem content: contents) {
            if(content.getSubtype() != null && content.getSubtype().equals("paragraph")) {
                firstFivePara = firstFivePara + " " + content.getContent().replaceAll("http.*?\\s", " ");
                count++;
            }
            if(count >= 5) {
                break;
            }
        }
        List<String> docTerms = tokenize.process(firstFivePara); // Tokenize Docterms

        HashMap<String, Integer> frequency = new HashMap<>();

        // Doc Term Frequencies
        for(String token: docTerms) {
            if(frequency.containsKey(token)) {
                frequency.replace(token, frequency.get(token) + 1);
            }
            else {
                frequency.put(token, 1);
            }
        }
        
        TokenFrequency frequency_object = new TokenFrequency(frequency);
        

        return new TokenizedNewsArticle(
                news.getId(),
                tokenizedTitle,
                docTerms.size(),
                frequency_object
                );
    }



}
