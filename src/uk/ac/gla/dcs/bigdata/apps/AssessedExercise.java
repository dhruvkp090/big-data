package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentLengthMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentLengthReducer;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleFilter;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsTokenizerMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TokenFrequencyReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//2 filters are applied here, 1. filters out all news articles without title, 2. calls custom filter function.
		Dataset<NewsArticle> filteredNews = news.filter(news.col("title").isNotNull()).filter(new NewsArticleFilter());

		// System.out.println(news.count());
		// System.out.println(filteredNews.count());
//		filteredNews.printSchema();

		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------


		Dataset<TokenizedNewsArticle> tokenNews = filteredNews.map(new NewsTokenizerMap(spark), Encoders.bean(TokenizedNewsArticle.class));
//
		List<TokenizedNewsArticle> tokenNewsAll = tokenNews.collectAsList();
		for(TokenizedNewsArticle c: tokenNewsAll) {
			System.out.println(c.getFrequency());
		}
		
		
		// Extract the lengths of the documents by performing a map from TokenizedNewsArticle to an integer (the length)
		Dataset<Integer> documentLengths = tokenNews.map(new DocumentLengthMap(), Encoders.INT());
		// Sum the documents' lengths in a parallel manner 
		// This will trigger processing up to this point 
		Integer DocLengthSum = documentLengths.reduce(new DocumentLengthReducer());
		// Calculate the number of documents to calculate the average
		int DocCount = (int) tokenNews.count();
		// Calculate the average
		float AvgDocLength =  DocLengthSum/DocCount;
		
		System.out.println(AvgDocLength);
		
		//below to commented code lines to calculate the map of all token frequences, encoder should be corrected
		// Dataset<Map<String,Integer>> tokenFrequencies = tokenNews.map(new DocumentLengthMap(), Encoders.());
		// Map<String,Integer> allTokenFrequencies = tokenFrequencies.reduce(new TokenFrequencyReducer());
		
		CorpusSummary detailsDataset = new CorpusSummary(DocCount, AvgDocLength, null); 
		
		
		
		
		
		
		
		
		
		return null; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}
