package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultQuery;

public class GetTop10 implements MapGroupsFunction<Query, RankedResultQuery, Tuple2<Query,DocumentRanking>> {

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Query, DocumentRanking> call(Query key, Iterator<RankedResultQuery> values) throws Exception {
		List<RankedResult> result = new ArrayList<>();
		
		while (values.hasNext()) {
			RankedResultQuery value = values.next();
			String title1 = value.getArticle().getTitle();
			double size = result.size();
			if(size==0) {
				result.add(new RankedResult(value.getDocid(),value.getArticle(),value.getScore()));
			}else {
				if(size<=10) {
					Boolean is_similar = false;
					for(RankedResult r : result) {
						String title2 = r.getArticle().getTitle();
						double distance = TextDistanceCalculator.similarity(title1, title2);
						if(distance<0.5) {
							is_similar = true;
							break;
							
						}
					}
					if(!is_similar) {
						result.add(new RankedResult(value.getDocid(),value.getArticle(),value.getScore()));
					}
					
				}else {
					break;
						
					
				}
				
			}

		}
		DocumentRanking docrankfinal = new DocumentRanking(key,result);
		
		return new Tuple2<Query,DocumentRanking> (key,docrankfinal);
	}
	

}
