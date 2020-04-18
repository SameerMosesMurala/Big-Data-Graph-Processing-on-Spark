import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection._

object Graph {
  def main(args: Array[ String ]) {
  val conf = new SparkConf().setAppName("Graph")
  val sc = new SparkContext(conf)
  conf.setMaster("local[2]")
  var graph = sc.textFile(args(0)).map( line => { val a = line.split(",")
   var list:Array[Long]=Array[Long]()
   var listelement=""
   for(listelement<-a){list=list:+listelement.toLong}
   (a(0).toLong,a(0).toLong, list.takeWhile(_>0))
  })
  graph.collect().foreach(println)
  var flatmap_op=graph.flatMap(graph =>{
  val vertices: Array[(Long,Long)]=new Array[(Long,Long)](graph._3.length+1)
  val adj: Array[Long]=graph._3
  vertices(0)=(graph._1,graph._2)
  for(i<- 0 until graph._3.length){
  vertices(i+1)=(adj(i),graph._2)
  }
  vertices
  }).reduceByKey((a, b) => if (a._1 < b._1) a else b)
  var graph_reconstruct=flatmap_op.map(flatmap_op => {
  (flatmap_op._1,flatmap_op._2,Array(0))
  })
  flatmap_op.collect().foreach(println)
  graph_reconstruct.collect().foreach(println)
  //graph=graph.join(graph_reconstruct)
  //graph = graph.map( graph => (graph._1,graph._2,graph._3)) .join(graph_reconstruct.map( graph_reconstruct => (graph_reconstruct._1,graph_reconstruct._2,graph_reconstruct._3) ))
  graph = graph.map( graph => (graph._1,(graph._2,graph._3))) .join(graph_reconstruct.map( graph_reconstruct => (graph_reconstruct._1,(graph_reconstruct._2,graph_reconstruct._3))))

  graph.collect().foreach(println)
  }
  }