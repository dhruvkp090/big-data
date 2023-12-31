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
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.*;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusSummary;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenFrequency;
import uk.ac.gla.dcs.bigdata.studentfunctions.TokenFrequencyMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.TokenFrequencyReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsArticle;

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
			newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 5000 news
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
	/**
	 * This function loads Json objects containing queries and news articles.
	 * It loops over the queries and adds its terms into a corpus object as a hashmap key.
	 * A TokenizedNewsArticle object is created from every entry in the news article json.
	 * Frequencies of all the terms in queries are then mapped and reduced to get the overall
	 * allTokenFrequencies hashmap. All corpus statistics are then broadcasted to the scorerMap
	 * inside a loop over queries. Inside this loop we also perform the reduction over DocumentRankings 
	 * gathered from scorerMap. The result of this reduction is then the final sorted DocumantRanking of size 10
	 * without similar articles that is added to the finalRankings list.
	 * 
	 * @param spark		Spark session
	 * @param queryFile A file containing a list of queries
	 * @param newsFile	A Json file that contains a collection of News Articles
	 * @return 			List of DocumentRankings of the most relevant documents per query
	 */
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
//		Get list of all queries and create a blank hash map
		List<Query> queryList = queries.collectAsList();
		List<String> queryTerms = new ArrayList<>();
		for (Query q : queryList) {
			queryTerms.addAll(q.getQueryTerms());
		}
		Broadcast<List<String>> broadcastQueryTerms = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTerms);
//		Return tokenized news articles using flatmap
		LongAccumulator totalDocLength = spark.sparkContext().longAccumulator();
		NewsTokenizerFlatMap newsFlatMapper = new NewsTokenizerFlatMap(broadcastQueryTerms,totalDocLength);
		Dataset<TokenizedNewsArticle> tokenizedNews = news.flatMap(newsFlatMapper, Encoders.bean(TokenizedNewsArticle.class));
		
//		map and reduce to get the overall term frequency across the whole corpus
		Dataset<TokenFrequency> tokenFrequencies = tokenizedNews.map(new TokenFrequencyMap(),Encoders.bean(TokenFrequency.class));
		TokenFrequency allTokenFrequencies = tokenFrequencies.reduce(new TokenFrequencyReducer());
		

//		get number of documents in the corpus
		long numberOfDocs = tokenizedNews.count();
//		Create a corpus summary structure
		CorpusSummary corpusSummary = new CorpusSummary(numberOfDocs, totalDocLength.value() / numberOfDocs, allTokenFrequencies);

		Broadcast<CorpusSummary> broadcastCorpus = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(corpusSummary);
		
//		Get Final rankings using a reducer
		List<DocumentRanking> finalRankings = new ArrayList<>();
		for (Query q : queryList) {
//			create DocumentRanking for all tokenizedNewsArticles
			Dataset<DocumentRanking> rankedDocuments = tokenizedNews.map(new ScorerMap(broadcastCorpus, q), Encoders.bean(DocumentRanking.class));
//			reduce the Rankings into the final one
			DocumentRanking output = rankedDocuments.reduce(new DocumentRankingReducer());
			finalRankings.add(output);
		}
		return finalRankings;

	}

}
