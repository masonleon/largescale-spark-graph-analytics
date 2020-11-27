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

    // Distances structure: (userID, distance)
    // This data will change each iteration
    // Set all distances for "first hop" to 1
    var distances = graph.values // List[(friendID)]
      .flatMap(friends => friends.map(id => (id, edgeWeight)))


    // How to calculate the shortest path?
    // All origin userIDs are nodes in the graph.  If there exists a shortest path to anywhere, it must start at an ID in the graph
    // On a shortest path, for all nodes in the path, the path is also the shortest path for those nodes
    // So we really just need to build up the paths from every possible starting ID?
    // And then after convergence go through and output the final results?  How?
    // Will every node get covered?

    for (iteration <- 0 to k) {
      distances = graph.rightOuterJoin(distances) // (userID, (Option[adjList], distance))
        .flatMap {
        case (id, (Some(adjList), distance)) => updateDistances(id, adjList, distance)
        case (id, (None, distance)) => List((id, distance))
      }
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any user
    }
    distances.saveAsTextFile(args(1))
  }

  /**
    * Pass the distance from given user to all of its adjacent nodes.
    *
    * @param userID   ID for this user
    * @param adjList  Iterable containing IDs for this user's adjacent nodes
    * @param distance total distance to this user
    * @return Iterable of (userID, distance) for this user and all of its adjacent nodes.
    */
  def updateDistances(userID: String, adjList: Iterable[String], distance: Int): Iterable[(String, Int)] = {
    adjList.map(id => (id, edgeWeight + distance)) ++ List((userID, distance))
  }
}
