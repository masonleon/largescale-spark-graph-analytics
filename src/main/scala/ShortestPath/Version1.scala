package ShortestPath

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import utils.GraphRDD.{generateGraphRDD, getNumEdges, saveSingleOutput}

object Version1 {

  /**
   * Weight for each edge connecting vertices in the graph.
   */
  val edgeWeight = 1

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nShortestPaths <input> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("ShortestPaths")

    val sc = new SparkContext(conf)

    // Input file: (userID, friendID)
    // Transform to...
    // Graph structure:  (userID, List[(friends)])
    // Graph structure is static --> persist or cache
    val graph = generateGraphRDD(sc, args(0), " ")
      .cache()

    val distances = apspRDD(graph)
    saveSingleOutput(distances, args(1) + "/distances")

    //    val gexf = getGexfRDD(sc, graph)
    //    saveGexfSingleOutput(gexf, args(1) + "/gexf")
  }

  /**
   * All-Pairs-Shortest-Path. Finds the minimum distance from any node to all other nodes in the
   * graph. For a graph of a social network, where each node represents a user and each edge
   * represents friendship/followership, the all-pairs shortest path (APSP) data would represent the
   * minimum “degree of separation” between each user. APSP can be used to determine “betweenness
   * centrality”, which is a measure for quantifying the control of a human on the communication
   * between other humans in a social network, or to determine graph “diameter”, which is a measure
   * of network size.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return DistancesRDD representing all-pairs-shortest-paths for graph G in RDD format as RDD[((V_to, List, V_from), Dist)])])
   */
  def apspRDD(GraphRDD: RDD[(String, Iterable[String])]) = {
    val k = getNumEdges(GraphRDD)

    // Distances structure: ((toId, fromId), distance)
    // This data will change each iteration
    var DistancesRDD = initializeDistances(GraphRDD, false)

    for (iteration <- 0 to k) {
      DistancesRDD = DistancesRDD.map { case ((toId, fromId), distance) =>
        (toId, (fromId, distance))
      }
        .leftOuterJoin(GraphRDD) // (toId, ((fromId, distance), Option[adjList]))
        .flatMap(x => updateDistances(x))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair
    }

    DistancesRDD
  }

  /**
   * Initialize the path distances structure for a graph.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)])
   * @param optimize set as true to explicitly use same partitioner for graph and distance structures
   * @return distances data as RDD[((toId, fromId), distance)]
   */
  def initializeDistances(GraphRDD: RDD[(String, Iterable[String])], optimize: Boolean) = {
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
   * Pass the distance from current node on to its adjacent nodes, while keeping the distance from
   * the current node.  Returns current distance info for current node with updated distance info
   * for all of its adjacent nodes.
   *
   * @param tuple representing (toId, (adjList, (fromId, distance)))
   * @return Iterable representing updated ((toId, fromId), distance)
   */
  def updateDistances(tuple: (String, ((String, Int), Option[Iterable[String]]))): Iterable[((String, String), Int)] = {
    tuple match {
      case (toId, ((fromId, distance), Some(adjList))) =>
        adjList.map(newId => ((newId, fromId), edgeWeight + distance)) ++ List(((toId, fromId), distance))
      case (toId, ((fromId, distance), None)) => List(((toId, fromId), distance))
    }
  }.filter { case ((toId, fromId), _) => !toId.equals(fromId) } // Don't keep circular distances

}
