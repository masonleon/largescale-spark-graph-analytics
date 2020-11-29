import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

    val conf = new SparkConf().setAppName("ShortestPaths")
    val sc = new SparkContext(conf)

    // TODO use same partitioner for graph and distances to reduce shuffling during join

    // Input file: (userID, friendID)
    // Transform to...
    // Graph structure:  (userID, List[(friends)])
    // Graph structure is static --> persist or cache
    val graph = generateGraphRDD(sc, args(0), " ")



//    distances.saveAsTextFile(args(1))

    val diameter = getDiameter(sc, graph)

    diameter
      .coalesce(1)
      .saveAsTextFile(args(1))
  }

  /**
   * Generate a graph G of pair (V, E) in adjacency list format as RDD, where V is set of vertices and E is
   * set of edges, such that E ⊆ V x V. Graph generated from input text file representing |E| where
   * each edge is record in text file of v1 -> v2.
   *
   * @param context   representing SparkContext
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @param separator representing the separator character such as ",", " ", "|", etc.*
   * @return cached graph G in adjacency list format as RDD[(V, List[(V)])
   */
  def asspRDD(context: SparkContext, GraphRDD: RDD[(String, Iterable[String])], separator: String): RDD[(String, (String, Int))] = {
    val k = getK(GraphRDD)

    // Distances structure: (toId, (fromId, distance))
    // This data will change each iteration
    // Set all distances for "first hop" to 1
    var distances = GraphRDD.flatMap { case (fromId, adjList) =>
      adjList.map(adjId => (adjId, (fromId, edgeWeight)))
    }

    // How to calculate the shortest path?
    // All origin userIDs are nodes in the graph.  If there exists a shortest path to anywhere, it must start at an ID in the graph
    // On a shortest path, for all nodes in the path, the path is also the shortest path for those nodes
    // So we really just need to build up the paths from every possible starting ID?
    // And then after convergence go through and output the final results?  How?
    // Will every node get covered?
    for (iteration <- 0 to k) {
      distances = GraphRDD.rightOuterJoin(distances) // (toId, (Option[adjList], (fromId, distance)))
        .flatMap(x => updateDistances(x))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair
        .map { case ((toId, fromId), distance) => (toId, (fromId, distance)) }
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
  def updateDistances(tuple: (String, (Option[Iterable[String]], (String, Int)))):
  Iterable[((String, String), Int)] = {
    tuple match {
      case (toId, (Some(adjList), (fromId, distance))) =>
        adjList.map(newId => ((newId, fromId), edgeWeight + distance)) ++ List(((toId, fromId), distance))
      case (toId, (None, (fromId, distance))) => List(((toId, fromId), distance))
    }
  }.filter { case ((toId, fromId), _) => !toId.equals(fromId) } // Don't keep circular distances
  //TODO incorporate filter logic in the match statement to make more efficient?

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
    val graph = context.textFile(inputFile)
      .map { line =>
        val tokens = line.split(separator)
        (tokens(0), tokens(1))
      }
      .groupByKey()
      .cache()

    graph
  }

  /**
   * Get number of iterations. K = |V|.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return k number of iterations for graph convergence.
   */
  def getK(GraphRDD: RDD[(String, Iterable[String])]): Int = {
    val k = GraphRDD.count().toInt

    k
  }

  /**
   * Get number of iterations. K = |V|.
   *
   * @param context representing SparkContext
   * @param DistancesRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   *
   * @return k number of iterations for graph convergence.
   */
  def getDiameter(context: SparkContext, DistancesRDD: RDD[(String, (String, Int))]) = {
//    val diameter = graph
//      .collect
//      .maxBy(_._2._2)
//      .collect.maxBy(_._2)

    val diameter = DistancesRDD
      .sortBy(_._2._2,ascending = false)
      .take(1)

    context.parallelize(diameter)
  }
}
