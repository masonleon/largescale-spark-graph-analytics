package ShortestPath

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object ShortestPaths {

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

    val diameter = getDiameter(sc, graph)
    saveSingleOutput(diameter, args(1) + "/diameter")

    val distances = apspRDD(graph)
    saveSingleOutput(distances, args(1) + "/distances")
  }

  //  def saveJSONOutput(spark: SparkSession, GraphRDD: RDD[(String, (String, Int))], outputFile: String) = {
  //    val df = spark.createDataFrame(distances)
  //    df.coalesce(1).write.json(args(1) + "/test.json")
  //  }

  /**
   * Helper function to save output in coalesced single text file.
   *
   * @param data       any RDD containing key (String, String) and value (Int)
   * @param outputFile representing string output file dir.
   */
  def saveSingleOutput(data: RDD[((String, String), Int)], outputFile: String) = {
    data
      .coalesce(1)
      .saveAsTextFile(outputFile)
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
    val k = getK(GraphRDD)

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
   * Generate a graph G of pair (V, E) in adjacency list format as RDD, where V is set of vertices and E is
   * set of edges, such that E ⊆ V x V. Graph generated from input text file representing |E| where
   * each edge is record in text file of v1 -> v2.
   *
   * @param context   representing SparkContext
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
      .groupByKey()
      .cache()

    graph
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
  def updateDistances(tuple: (String, ((String, Int), Option[Iterable[String]]))):
  Iterable[((String, String), Int)] = {
    tuple match {
      case (toId, ((fromId, distance), Some(adjList))) =>
        adjList.map(newId => ((newId, fromId), edgeWeight + distance)) ++ List(((toId, fromId), distance))
      case (toId, ((fromId, distance), None)) => List(((toId, fromId), distance))
    }
  }.filter { case ((toId, fromId), _) => !toId.equals(fromId) } // Don't keep circular distances

  /**
   * Get graph diameter. The diameter is defined as the longest path in the set of all-pairs
   * shortest paths in the graph and is a common measure of network size. In a social network graph,
   * a small diameter would indicate a high degree of connectivity between members (no one person
   * has too many degrees of separation from another).
   *
   * @param context  representing SparkContext
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return diameter of graph.
   */
  def getDiameter(context: SparkContext, GraphRDD: RDD[(String, Iterable[String])]) = {
    val DistancesRDD = apspRDD(GraphRDD)

    val diameter = DistancesRDD
      .sortBy(_._2, ascending = false)
      .take(1)

    context
      .parallelize(diameter)
  }
}
