2 parts : PageRank on Spark and Twitter on Structured Spark Streaming

A.PageRank application

Compute the PageRank for a number websites and analyze the performance of your application under different
scenarios.
PageRank is an algorithm that is used by Google Search to rank websites in their search engine results. This
algorithm iteratively updates a rank for each document by adding up contributions from documents that link to
it. 
The algorithm can be summarized in the following steps -
Start each page at a rank of 1.
On each iteration, have page p contribute rank(p)/|neighbors(p)| to its neighbors.
Set each page's rank to 0.15 + 0.85 X contributions.

To run:

1. ./PartAQuestion1.sh
2. ./PartAQuestion2.sh
3. ./PartAQuestion3.sh

Used scala API to implement map reduce
NOTE:
In PartAQ3, had to increase executor memory to 4GB since persist() was rather delaying the application at 1GB


B.Structured Streaming on Twitter DataSets
Developing simple streaming applications on big-data frameworks

1.cd grader
2. ./PartBQuestion1.sh
3. ./PartBQuestion2.sh
4. ./PartBQuestion3.sh
