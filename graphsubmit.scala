import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._

object Graph1 {
  def main ( args: Array[String] ) {
val conf = new SparkConf().setAppName("Graph1")
  val sc = new SparkContext(conf)
  conf.setMaster("local[2]")
   var graph = sc.textFile(args(0)).map( line => { val a = line.split(",")
   var list:Array[Long]=Array[Long]()
   var listelement=""
   for(listelement<-a){list=list:+listelement.toLong}
   (a(0).toLong,a(0).toLong, list.drop(1))
  })
  //graph.collect().foreach(println)
  var graph1 : RDD[Edge[Long]] =graph.flatMap(graph =>{                                      
  val vertices: Array[(Long,Long)]=new Array[(Long,Long)](graph._3.length + 1)
  val adjacent: Array[Long]=graph._3
  vertices(0)=(graph._1,graph._2)
  for(i<- 0 until graph._3.length){
  vertices(i+1)=(adjacent(i),graph._2)
  }
  vertices
  }).map({ case(a,b)=> Edge(a,b)})
//graph1.collect().foreach(println)
 //val graph2 : Graph[Any, Long] = Graph.fromEdges(graph1, "defaultProperty")
val graph2 : Graph[Any, Long] = Graph.fromEdges(graph1,1) 
//graph2.triplets.collect.foreach(println)
//graph2.vertices.collect.foreach(println)
//graph2.edges.collect.foreach(println)
 val initialGraph = graph2.mapVertices((id, _) =>
    id)
val sssp = initialGraph.pregel(5)(
  (id, groupid, newgroupid) => math.min(groupid, newgroupid), // Vertex Program
  triplet => {  // Send Message
    if (triplet.srcAttr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr.toInt))
    } else {
      Iterator.empty
    }
  },
  (a, b) => math.min(a, b) // Merge Message
)
//initialGraph.triplets.collect.foreach(println)
//initialGraph.vertices.collect.foreach(println)
//initialGraph.edges.collect.foreach(println)
//sssp.vertices.collect.foreach(println)
val groupcount=sssp.vertices.map(x =>(x._2,1)).reduceByKey(_+_)
val output=groupcount.collect()
output.foreach(println)
sc.stop()
  }
}
 
