## Big Data Report

### Contributors
- Martin Nahalka (2819881N) - martin.nahalka.14@aberdeen.ac.uk, martttinhobc@gmail.com, 2819881n@student.gla.ac.uk
- Dhruv Kumar Patwari (2782262P) - 2782262p@student.gla.ac.uk
- Melonie de Almeida (2813610D) - melonienimashadealmeida@melonies-macbook-air.local, 2813610d@student.gla.ac.uk 

### Program Logic Summary
This program aims to rank the documents in the corpus concerning the queries using DPH scores. To achieve this, we first filter out any documents that do not have a title or have no contents from the list. The remaining documents are then tokenized, and the frequency of only the terms in the list of queries are stored in a hashmap. Both the steps mentioned earlier are performed using a single flatmap which returns a TokenizedNewsArticle object. A CorpusSummary object is created with the help of a map and a reduce function. We then loop over all the queries, constructing a DocumentRanking object using a scorer map. This dataset is then reduced to get a final list of documents sorted in descending order wrt the DPH scores, and any similar documents are removed from the list.
### Custom Functions
 
#### NewsTokenizerFlatMap
This class uses the flatmap interface, which filters the documents without titles or content and calculates the term frequencies of the documents for terms in the queries.
#### ScorerMap
This MapFunction calculates DPH score for a given TokenizedNewsArticle object w.r.t  Query Score is calculated for each term in the query, which is then summed and divided by the number of terms in the query. This score is used to create a RankedResult object that is then used to create a DocumentRanking object that contains the query.
#### DocumentRankingReducer
This reducer reduces 2 objects of type DocumentRanking into 1. For every pair of DocumentRankings, assume that these are sorted and take the biggest element of dr2 and compare it with the smallest element of dr1; if true, it checks whether the element from dr2 is similar to any of the elements in dr1. If true(i.e. not similar), then the biggest element of dr2 replaces the smallest element of dr1 in dr1 and dr1 is sorted. Then we compare the currently smallest element of dr1 with the currently biggest element of dr2.
#### TokenFrequencyMap
This Map function extracts the token frequency pairs of a document.
#### TokenFrequencyReducer
This ReduceFunction merges two hashmaps of token frequency pairs. This is processed recursively for the whole dataset to get the sum of term frequencies for the term across all documents.

![Graph](/images/graph.jpg "graph")
The diagram above shows how different custom functions fit in the pipeline and how inputs are mapped into the final results through multiple intermediate outputs.

### Efficiency Discussion
To approach this task efficiently, we had to ponder how to efficiently store the data we needed to process, avoid any duplicate data, and use as few spark transformations as possible to accomplish the task. We have opted to store only the terms contained within the queries, as the other ones not in the queries are only counted but not stored. We are only storing the data necessary for the DPH score computation. We utilise one flat map to perform tokenising and filtering and compute the frequencies because we don't want a return for each article. We then use the map and reduce to get the token frequency for given query terms across the whole corpus. Lastly, we use a single loop over queries where a reduction of document rankings is executed and return the output for a given query.
### Challenges
We encountered several challenges whilst completing this project. One of which was how to store the data we needed to process. We have met the Java Heap Exception multiple times when running our first solution on the large dataset. Two factors caused this. Initially, we stored all tokens of the news article inside the TokenizedNewsArticle object. Most of these were never used, as we only need the ones occurring in the queries to be passed to the DPH scorer. The second factor was creating an extensive list of RankedResults for all the queries, which we partitioned with the map by query and then reduced by the query. As this was causing another JavaError of lists being too long, we have decided to loop over queries and perform reduction per query and add the final result per query into the list that accumulates all the results. Another challenge was to get the overall term frequency for the whole corpus. We have tested solutions with accumulators but failed to make it work, as there doesn't exist an accumulator for hashmap, so we have opted for another map-reduce.



