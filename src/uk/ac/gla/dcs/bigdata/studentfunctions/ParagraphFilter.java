package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ParagraphFilter implements FlatMapFunction<ContentItem, String> {

    @Override
    public Iterator<String> call(ContentItem contentItem) throws Exception {
        List<String> text;
        if (contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph")) {
            text = new ArrayList<String>(1);
            text.add(contentItem.getContent());
        }
        else {
            text = new ArrayList<String>(0);
        }
        return text.iterator();
    }
}
