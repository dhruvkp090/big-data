## Big Data Report

### Contributors
- Martin Nahalka (2819881N) - martin.nahalka.14@aberdeen.ac.uk, martttinhobc@gmail.com, 2819881n@student.gla.ac.uk
- Dhruv Kumar Patwari (2782262P) - 2782262p@student.gla.ac.uk
- Melonie de Almeida (2813610D) - melonienimashadealmeida@melonies-macbook-air.local, 2813610d@student.gla.ac.uk 

### Program Logic Summary
Our overall program performs a map over the documents to calculate the term frequency and document length of each token within 
each tokenised document and store it in a new class called TokenizedNewsArticle. Before the map, we filter out documents that do 
not have a title, has less than 5 paragraphs and only select the first 5 paragraphs if the documents have more than 5 
paragraphs. A corpus summary is then calculated by accumulating all the token frequencies and averaging document lengths. We now 
have all the required values to calculate the DPH score, and then we sort the document in descending order of the scores. We 
then select the first 10 non-similar documents by calculating the distances between the titles of each document. This list is 
then returned to the main function, where we create a file for each query consisting of the top 10 similar documents in the 
corpus.
### Custom Functions
 
#### NewsTokenizerFlatMap
This is a class that uses the flatmap interface which filters the documents without title or content and  calculates the term 
frequencies of the documents for terms in the queries.
#### ScorerMap
This MapFunction calculates DPH score for a given TokenizedNewsArticle object w.r.t  Query Score is calculated for each term in 
the query, which is then summed and divided by the number of terms in the query. This score is used to create a RankedResult 
object that is then used to create a DocumentRanking object that also contains the query.
#### DocumentRankingReducer
This reducer reduces 2 objects of type DocumentRanking into 1. For every pair of DocumentRankings, assume that these are sorted 
and take the biggest element of dr2 and compare with the smallest element of dr1 if true, it checks whether the element from dr2 
is similar to any of the elements in dr1, if true(i.e. not similar) then the biggest element of dr2 replaces the smallest 
element of dr1 in dr1 and dr1 is sorted. Then we move on to compare the currently smallest element of dr1 with the currently 
biggest element of dr2.
#### TokenFrequencyMap
This Map function extracts the token frequency pairs of a document.
#### TokenFrequencyReducer
This ReduceFunction merges two hashmaps of token frequency pairs, this is processed recursively for the whole dataset to get sum 
of term frequencies for the term across all documents

![Graph](/images/graph.jpg "graph")

### Efficiency Discussion
In order to approach this task efficiently, we had to ponder how to efficiently store the data that we needed to process and avoid any duplicate data and also use as few spark transformations as possible to accomplish the task. We have opted to store only those terms that are contained within the queries as the other ones that are not in the queries are only counted but not stored. We are only storing the data necessary for the DPH score computation. We utilize one flat map to perform tokenizing and filtering and computing the frequencies because we don't want a return for each article. We then use map and reduce to get the token frequency for given query terms across the whole corpus. Lastly we use a single loop over queries where a reduce of document rankings is executed and return the output for a given query.

### Challenges
We have encountered a number of challenges whilst completing this project. One of which was how to store the data we need to process. We have encountered the Java Heap Exception multiple times when running our first solution on the big dataset. This was caused by two factors, initially we have stored all tokens of the news article inside the TokenizedNewsArticle object, most of this were never used as we only need the ones that are occurring in the queries for the DPH scorer. The second factor was us creating a big list of RankedResults for all queries, which we then partitioned with mapby query and then reduced by query. As this was causing another JavaError of lists being too long we have decided to loop over queries and perform reduction per query and adding the final result per query into the list that accumulates all the results. Another challenge was to get the overall term frequency for the whole corpus. We have tested solutions with accumulators but failed to make it work, as there doesn't exist an accumulator for hashmap, so we have opted for another map-reduce.




