Project Description
Graph Processing using Spark and Scala. 
The graph can be represented as RDD[ ( Long, Long, List[Long] ) ], where the first Long is the graph node ID,
the second Long is the group that this vertex belongs to (initially, equal to the node ID), and the List[Long] is 
the adjacent list (the IDs of the neighbors). Here is the pseudo-code:

var graph = /* read the graph from args(0); the group of a graph node is set to the node ID */

for (i <- 1 to 5)
   graph = graph.flatMap{ /* associate each adjacent neighbor with the node group number + the node itself with its group number*/ }
                .reduceByKey( /* get the min group of each node */ )
                .join( /* join with the original graph */ )
                .map{ /* reconstruct the graph topology */ }

/* finally, print the group sizes */
