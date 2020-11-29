import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPaths {

  /**
    * Number of iterations.
    */
  val k = 4 // TODO is there a better way to make sure the graph converges other than running |V| iterations?
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
    val graph = sc.textFile(args(0))
      .map { line =>
        val tokens = line.split(" ")

        (tokens(0), tokens(1))
      }
      .groupByKey()
      .cache()

    // Distances structure: (toId, (fromId, distance))
    // This data will change each iteration
    // Set all distances for "first hop" to 1
    var distances = graph.flatMap { case (fromId, adjList) =>
      adjList.map(adjId => (adjId, (fromId, edgeWeight)))
    }


    // How to calculate the shortest path?
    // All origin userIDs are nodes in the graph.  If there exists a shortest path to anywhere, it must start at an ID in the graph
    // On a shortest path, for all nodes in the path, the path is also the shortest path for those nodes
    // So we really just need to build up the paths from every possible starting ID?
    // And then after convergence go through and output the final results?  How?
    // Will every node get covered?

    for (iteration <- 0 to k) {
      distances = graph.rightOuterJoin(distances) // (toId, (Option[adjList], (fromId, distance)))
        .flatMap(x => updateDistances(x))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair
          .map { case ((toId, fromId), distance) => (toId, (fromId, distance)) }
    }

    distances.saveAsTextFile(args(1))
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
}
