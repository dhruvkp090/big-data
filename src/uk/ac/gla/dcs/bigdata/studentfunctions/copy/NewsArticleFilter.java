package uk.ac.gla.dcs.bigdata.studentfunctions.copy;

import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
/**
 * Filters out all news articles that have less than 5 paragraphs
 *
 */
public class NewsArticleFilter implements FilterFunction<NewsArticle> {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean call(NewsArticle value) throws Exception {
		
		
		List<ContentItem> contents = value.getContents();
		int count = 0;
        for(ContentItem content: contents) {
            if(content.getSubtype() != null && content.getSubtype().equals("paragraph")) {
                count++;
            }
            if(count >= 5) {
                break;
            }
        }
        if (count < 5){
        	return false;
        }
		return true;
	}

}
