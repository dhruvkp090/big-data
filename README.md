Big Data Report
Program Logic Summary
Our overall program performs a map over the documents to calculate the term frequency and document length of each token within 
each tokenised document and store it in a new class called TokenizedNewsArticle. Before the map, we filter out documents that do 
not have a title, has less than 5 paragraphs and only select the first 5 paragraphs if the documents have more than 5 
paragraphs. A corpus summary is then calculated by accumulating all the token frequencies and averaging document lengths. We now 
have all the required values to calculate the DPH score, and then we sort the document in descending order of the scores. We 
then select the first 10 non-similar documents by calculating the distances between the titles of each document. This list is 
then returned to the main function, where we create a file for each query consisting of the top 10 similar documents in the 
corpus.
​​Custom Functions
NewsTokenizerFlatMap
This is a class that uses the flatmap interface which filters the documents without title or content and  calculates the term 
frequencies of the documents for terms in the queries.
ScorerMap
This MapFunction calculates DPH score for a given TokenizedNewsArticle object w.r.t  Query Score is calculated for each term in 
the query, which is then summed and divided by the number of terms in the query. This score is used to create a RankedResult 
object that is then used to create a DocumentRanking object that also contains the query.


DocumentRankingReducer
This reducer reduces 2 objects of type DocumentRanking into 1. For every pair of DocumentRankings, assume that these are sorted 
and take the biggest element of dr2 and compare with the smallest element of dr1 if true, it checks whether the element from dr2 
is similar to any of the elements in dr1, if true(i.e. not similar) then the biggest element of dr2 replaces the smallest 
element of dr1 in dr1 and dr1 is sorted. Then we move on to compare the currently smallest element of dr1 with the currently 
biggest element of dr2.
TokenFrequencyMap
This Map function extracts the token frequency pairs of a document.
TokenFrequencyReducer
This ReduceFunction merges two hashmaps of token frequency pairs, this is processed recursively for the whole dataset to get sum 
of term frequencies for the term across all documents

![Graph](/images/graph.jpg "graph")



