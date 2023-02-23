package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

import java.util.ArrayList;
import java.util.List;

public class DocMapper implements MapFunction<DocumentRanking, DocumentRanking> {
    @Override
    public DocumentRanking call(DocumentRanking value) throws Exception {
        List<RankedResult> filtered = new ArrayList<>();
        for (RankedResult r: value.getResults()) {
            if(filtered.size() == 0) filtered.add(r);
            else if (filtered.size() < 10) {
                Boolean is_similar = false;
                for(RankedResult f : filtered) {
                    String title2 = f.getArticle().getTitle();
                    String title1 = r.getArticle().getTitle();
                    double distance = TextDistanceCalculator.similarity(title1, title2);
                    if(distance<0.5) {
                        is_similar = true;
                        break;
                    }
                }
                if(!is_similar) {
                    filtered.add(r);
                }

            }
            else break;
        }
        return new DocumentRanking(value.getQuery(), filtered);
    }
}
