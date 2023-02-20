package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentLengthMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentLengthReducer;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleFilter;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsTokenizerMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TokenFrequencyMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TokenFrequencyReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

import static org.apache.spark.sql.functions.desc;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get
														// an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark
																			// finds it

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef == null)
			sparkMasterDef = "local[2]"; // default is local mode with two executors

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
		if (queryFile == null)
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile == null)
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news
																				// articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/" + System.currentTimeMillis());
			if (!outDirectory.exists())
				outDirectory.mkdir();

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

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts
																										// each row into
																										// a Query

		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this
																											// converts
																											// each row
																											// into a
																											// NewsArticle

		// 2 filters are applied here, 1. filters out all news articles without title,
		// 2. calls custom filter function.
		Dataset<NewsArticle> filteredNews = news.filter(news.col("title").isNotNull())
				.filter(new NewsArticleFilter());

		// System.out.println(news.count());
		// System.out.println(filteredNews.count());
		// filteredNews.printSchema();

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		Dataset<TokenizedNewsArticle> tokenNews = filteredNews.map(new NewsTokenizerMap(spark),
				Encoders.bean(TokenizedNewsArticle.class));
		//
		// List<TokenizedNewsArticle> tokenNewsAll = tokenNews.collectAsList();
		// for(TokenizedNewsArticle c: tokenNewsAll) {
		// System.out.println(c.getFrequency());
		// }

		// Extract the lengths of the documents by performing a map from
		// TokenizedNewsArticle to an integer (the length)
		Dataset<Integer> documentLengths = tokenNews.map(new DocumentLengthMap(), Encoders.INT());
		// Sum the documents' lengths in a parallel manner
		// This will trigger processing up to this point
		Integer DocLengthSum = documentLengths.reduce(new DocumentLengthReducer());
		// Calculate the number of documents to calculate the average
		int DocCount = (int) tokenNews.count();
		// Calculate the average
		float AvgDocLength = DocLengthSum / DocCount;

		// Extract the token frequencies of the documents by performing a map from
		// TokenizedNewsArticle to a TokenFrequency object
		Dataset<TokenFrequency> tokenFrequencies = tokenNews.map(new TokenFrequencyMap(),
				Encoders.bean(TokenFrequency.class));
		// Merge the token frequencies to get sum of term frequencies for the term
		// across all documents in a parallel manner
		TokenFrequency allTokenFrequencies = tokenFrequencies.reduce(new TokenFrequencyReducer());

		// Create a CorpusSummary object which contains total number of documents,
		// average document length and total token frequecies
		CorpusSummary detailsDataset = new CorpusSummary(DocCount, AvgDocLength, allTokenFrequencies);

		Broadcast<CorpusSummary> broadcastCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(detailsDataset);
		
		//Document Ranking objects for all the queries
		List<DocumentRanking> rankedQueries = new ArrayList<>();

		List<Query> queryList = queries.collectAsList();
		for (Query q : queryList) {
			Dataset<RankedResult> rankedDocuments = tokenNews.map(new ScorerMap(broadcastCorpus, q),
					Encoders.bean(RankedResult.class));
			rankedQueries.add(new DocumentRanking(q, rankedDocuments.collectAsList())); //Document ranking keeps only 10 reults, need to alter this
		}
		
		// Redundancy filtered and Sorted Document Ranking objects for all the queries
		List<DocumentRanking> rankedfilteredQueries = new ArrayList<>();
		 // Iterate over all the queries
		for (DocumentRanking dr : rankedQueries) { 
			// Get the list of Ranked Result Objects for the query in consideration
			List<RankedResult> results = dr.getResults(); 
			// Get the corresponding Query Object
			Query current_query = dr.getQuery();
			// Create a dataset of Ranked Result Objects
			Dataset<RankedResult> queryResults = spark.createDataset(results, Encoders.bean(RankedResult.class));
			//Returns x best ranked results in descending order
			Dataset<RankedResult> sorted = queryResults.sort(desc("score"));
			// Action that gets the list of all the Ranked Result Objects
			List<RankedResult> sortedList = sorted.collectAsList();
			
			// Create a new list of Ranked Result Objects for top items without redundancy
			List<RankedResult> filteredList = new ArrayList<>();
			//Add the top most item to the list, because it should anyway be in the list as it is the best match according to DH score
			filteredList.add(sortedList.get(0));
			//The current considered item number in the sortedList
			Integer num = 1; 
			
			// Loop till the number of items become the required number
			while(filteredList.size() < 10) { 
				// Handling the error when the num is higher than the (length 0f sortedList-1)	
				if (num >= sortedList.size()) {
					System.out.println("Warning: List of out of index before filtering 10 documents");
					System.out.println("Current size of filteredList: " + filteredList.size());
					break;
				}
				//Get the title 0f the article
				String title1 = sortedList.get(num).getArticle().getTitle();
				// Variable thatThe current considering article title is not similar to any of the title of articles in the final filteredList
				Boolean not_similar = true; 
				
				// Check if there is any similar article title in the filteredList
				for (RankedResult doc :filteredList ){
					String title2 = doc.getArticle().getTitle();
					double distance = TextDistanceCalculator.similarity(title1, title2);
					if (distance<0.5) {
						not_similar = false; //If there is a similar article title
						num++;
						break;
					}
				}
				
				// If there are no similar titles in the list, the Ranked Result Object is added to the filteredList
				if (not_similar) {
					filteredList.add(sortedList.get(num));
					num++;
					}	
			}
			// Create the Document Ranking Object for the query and filtered sorted list of RankedResult objects
			DocumentRanking dr_new = new DocumentRanking(current_query,filteredList);
			// Add the Document Ranking object to the output list
			rankedfilteredQueries.add(dr_new);
			
			System.out.println("Process done for Query: " + current_query.getOriginalQuery());
		}

		return rankedfilteredQueries; // replace this with the the list of DocumentRanking output by your topology
	}

}
