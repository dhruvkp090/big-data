package uk.ac.gla.dcs.bigdata.studentfunctions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenisedNewsArticle;

import java.io.Serializable;
import java.util.List;

public class NewsTokeniserMap implements MapFunction<NewsArticle, TokenisedNewsArticle> {

    private SparkSession spark;


    public NewsTokeniserMap(SparkSession spark) {
        this.spark = spark;
    }

    @Override
    public TokenisedNewsArticle call(NewsArticle news) throws Exception {
        TextPreProcessor tokenize = new TextPreProcessor();
        List<String> tokenizedTitle = tokenize.process(news.getTitle());
        List<ContentItem> check = news.getContents();
        Dataset<ContentItem> contents = spark.createDataset(check, Encoders.bean(ContentItem.class));
        Dataset<String> content = contents.flatMap(new ParagraphFilter(), Encoders.STRING());
        List<String> texts = content.collectAsList();
        System.out.println("Before Slice: " + texts.size());
        texts = texts.subList(0, 6);
        System.out.println("After Slice: " + texts.size());
        String firstFivePara = StringUtils.join(texts, " ");

        System.out.println(firstFivePara + "\n -------------------- ");
//        group by for tokens
        return new TokenisedNewsArticle(
                news.getId(),
                tokenizedTitle,
                0,
                null
                );
    }



}
