package ShortestPath
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}
import FilterUtils.passesFilterTest

object PathUtils {

  /**
    * Generate a graph G of pair (V, E) in adjacency list format as RDD, where V is set of vertices and E is
    * set of edges, such that E ⊆ V x V. Graph generated from input text file representing |E| where
    * each edge is record in text file of v1 -> v2.
    *
    * @param context representing SparkContext
    * @param inputFile representing argument for input file
    * @param separator representing the separator character such as ",", " ", "|", etc.*
    * @return cached graph G in adjacency list format as RDD[(V, List[(V)])
    */
  def generateGraphRDD(context: SparkContext, inputFile: String, separator: String): RDD[(String, Iterable[String])] = {
    val graph = context
      .textFile(inputFile)
      .map { line =>
        val tokens = line.split(separator)
        (tokens(0), tokens(1))
      }
      .filter(passesFilterTest)
      .groupByKey()
      .cache()

    graph
  }

  /**
    * Initialize the path distances structure for a graph.
    * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)])
    * @param optimize set as true to explicitly use same partitioner for graph and distance structures
    * @return distances data as RDD[((toId, fromId), distance)]
    */
  def initializeDistances(GraphRDD: RDD[(String, Iterable[String])], edgeWeight: Int, optimize: Boolean) = {
    // Set all distances for "first hop" to 1
    val distances = GraphRDD.flatMap { case (fromId, adjList) =>
      adjList.map(adjId => ((adjId, fromId), edgeWeight))
    }

    if (optimize) {
      val graphPartitioner = GraphRDD.partitioner match {
        case Some(p) => p
        case None => new HashPartitioner(GraphRDD.partitions.length)
      }
      distances.partitionBy(graphPartitioner)
    }

    distances
  }

  /**
    * Get number of iterations. K = |V|.
    *
    * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
    * @return k number of iterations for graph convergence.
    */
  def getK(GraphRDD: RDD[(String, Iterable[String])]): Int = {
    val k = GraphRDD
      .count()
      .toInt

    k
  }

  /**
    * Pass the distance from current node on to its adjacent nodes, while keeping the distance from
    * the current node.  Returns current distance info for current node with updated distance info
    * for all of its adjacent nodes.
    *
    * @param tuple representing (toId, (adjList, (fromId, distance)))
    * @return Iterable representing updated ((toId, fromId), distance)
    */
  def updateDistances(tuple: (String, ((String, Int), Option[Iterable[String]])), edgeWeight: Int):
  Iterable[((String, String), Int)] = {
    tuple match {
      case (toId, ((fromId, distance), Some(adjList))) =>
        adjList.map(newId => ((newId, fromId), edgeWeight + distance)) ++ List(((toId, fromId), distance))
      case (toId, ((fromId, distance), None)) => List(((toId, fromId), distance))
    }
  }.filter { case ((toId, fromId), _) => !toId.equals(fromId) } // Don't keep circular distances

  /**
    * Helper function to save output in coalesced single text file.
    *
    * @param data any RDD containing key (String, String) and value (Int)
    * @param outputFile representing string output file dir.
    */
  def saveSingleOutput(data: RDD[((String, String), Int)], outputFile: String) = {
    data
      .coalesce(1)
      .saveAsTextFile(outputFile)
  }
}
