import ood.GraphRDD.generateGraphRDD
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

    val conf = new SparkConf()
      .setAppName("ShortestPaths")

    val sc = new SparkContext(conf)

    // TODO use same partitioner for graph and distances to reduce shuffling during join
    // Input file: (userID, friendID)
    // Transform to...
    // Graph structure:  (userID, List[(friends)])
    // Graph structure is static --> persist or cache
    val graph = generateGraphRDD(sc, args(0), " ")

    val diameter = getDiameter(sc, graph)
    saveSingleOutput(diameter, args(1) + "/diameter")

    val distances = asspRDD(graph)
    saveSingleOutput(distances, args(1) + "/distances")

    val gexf = getGexfRDD(sc, graph)
    saveGexfSingleOutput(gexf, args(1) + "/gexf")

  }

  def saveGexfSingleOutput(GexfRDD: RDD[String], outputFile: String) = {
    GexfRDD
      .coalesce(1)
      .saveAsTextFile(outputFile)
  }

  /**
   * adapted from 4.3.3. GEXF format for Gephi visualization software
   * https://livebook.manning.com/book/spark-graphx-in-action/chapter-4/ch04lev2sec6
   * https://livebook.manning.com/book/spark-graphx-in-action/chapter-4/point-9169-150-150-0
   * @param
   * @return
   */
  def getGexfRDD(context: SparkContext, GraphRDD: RDD[(String, Iterable[String])]): RDD[String] = {
    val xml =
      "<?xml " +
        "version=\"1.0\" " +
        "encoding=\"UTF-8\"" +
      "?>\n" +
      "<gexf " +
        "xmlns=\"http://www.gexf.net/1.2draft\" " +
        "version=\"1.2\"" +
      ">\n" +
      "  " +
        "<graph " +
          "mode=\"static\" " +
          "defaultedgetype=\"directed\"" +
        ">\n" +
      "    " +
          "<nodes>\n" +
            GraphRDD
              .map(v =>
      "     " +
            "<node " +
              "id=\"" + v._1 + "\" " +
              "label=\"" + v._1 + "\" " +
            "/>\n"
              ).collect.mkString +
      "    " +
          "</nodes>\n" +
      "    " +
          "<edges>\n" +
            GraphRDD
              .flatMap{ case (v, adjList) =>
                adjList
                  .map(
                    adjId =>
      "      " +
            "<edge " +
              "source=\"" + v + "\" " +
              "target=\"" + adjId + "\" " +
              "weight=\"" + edgeWeight + "\" " +
            "/>\n"
                  )
              }.collect.mkString +
      "    " +
          "</edges>\n" +
      "  " +
        "</graph>\n" +
      "</gexf>"

    context
      .parallelize(List(xml))
  }

  /**
   * Helper function to save output in coalesced single text file.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @param outputFile representing string output file dir.
   */
  def saveSingleOutput(GraphRDD: RDD[(String, (String, Int))], outputFile: String) = {
    GraphRDD
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
   * @return DistancesRDD representing all-pairs-shortest-paths for graph G in RDD format as RDD[(V_to, List[(V_from, Dist)])])
   */
  def asspRDD(GraphRDD: RDD[(String, Iterable[String])]) = {
    val k = getK(GraphRDD)

    // Distances structure: (toId, (fromId, distance))
    // This data will change each iteration
    // Set all distances for "first hop" to 1
    var DistancesRDD = GraphRDD
      .flatMap { case (fromId, adjList) =>
        adjList
          .map(adjId => (adjId, (fromId, edgeWeight)))
      }

    // How to calculate the shortest path?
    // All origin userIDs are nodes in the graph.  If there exists a shortest path to anywhere, it must start at an ID in the graph
    // On a shortest path, for all nodes in the path, the path is also the shortest path for those nodes
    // So we really just need to build up the paths from every possible starting ID?
    // And then after convergence go through and output the final results?  How?
    // Will every node get covered?
    for (iteration <- 0 to k) {
      DistancesRDD = GraphRDD
        .rightOuterJoin(DistancesRDD) // (toId, (Option[adjList], (fromId, distance)))
        .flatMap(x => updateDistances(x))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair
        .map { case ((toId, fromId), distance) => (toId, (fromId, distance)) }
    }

    DistancesRDD
  }

  /**
   * Pass the distance from current node on to its adjacent nodes, while keeping the distance from
   * the current node.  Returns current distance info for current node with updated distance info
   * for all of its adjacent nodes.
   *
   * @param tuple representing (toId, (adjList, (fromId, distance)))
   * @return Iterable representing updated ((toId, fromId), distance)
   */
  def updateDistances(tuple: (String, (Option[Iterable[String]], (String, Int)))): Iterable[((String, String), Int)] = {
    tuple match {
      case (toId, (Some(adjList), (fromId, distance))) =>
        adjList
          .map(newId => ((newId, fromId), edgeWeight + distance)) ++ List(((toId, fromId), distance))
      case (toId, (None, (fromId, distance))) => List(((toId, fromId), distance))
    }
  }.filter { case ((toId, fromId), _) => !toId.equals(fromId) } // Don't keep circular distances
  //TODO incorporate filter logic in the match statement to make more efficient?



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
   * Get graph diameter. The diameter is defined as the longest path in the set of all-pairs
   * shortest paths in the graph and is a common measure of network size. In a social network graph,
   * a small diameter would indicate a high degree of connectivity between members (no one person
   * has too many degrees of separation from another).
   *
   * @param context representing SparkContext
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return diameter of graph.
   */
  def getDiameter(context: SparkContext, GraphRDD: RDD[(String, Iterable[String])]) = {
    val DistancesRDD = asspRDD(GraphRDD)

    val diameter = DistancesRDD
      .sortBy(_._2._2, ascending = false)
      .take(1)

    context
      .parallelize(diameter)
  }
}
