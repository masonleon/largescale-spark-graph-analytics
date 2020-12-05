import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPathsPartitioners {

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
        val tokens = line.split(" ")  // TODO change to "\t" for the real data

        (tokens(0), tokens(1))
      }
      .groupByKey()
      .cache()

    // Distances structure: ((toId, fromId), distance)
    // This data will change each iteration
    // Set all distances for "first hop" to 1
    var distances = graph.flatMap { case (fromId, adjList) =>
      adjList.map(adjId => ((adjId, fromId), edgeWeight))
    }

    var hasConverged = false
    while (!hasConverged) {
      val temp = distances.map { case ((toId, fromId), distance) => (toId, (fromId, distance)) }
        .leftOuterJoin(graph)   // (toId, ((fromId, distance), Option[adjList]))
        .flatMap(x => updateDistances(x))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair

      val numUpdated = temp.leftOuterJoin(distances)  // ((to, from), newDist, Option[oldDist])
        .filter {
        case (_, (newDist, Some(oldDist))) => newDist != oldDist
        case (_, (_, None)) => true
      }.count()

      if (numUpdated == 0) {
        hasConverged = true
      }

      distances = temp
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
  def updateDistances(tuple: (String, ((String, Int), Option[Iterable[String]]))):
  Iterable[((String, String), Int)] = {
    tuple match {
      case (toId, ((fromId, distance), Some(adjList))) =>
        adjList.map(newId => ((newId, fromId), edgeWeight + distance)) ++ List(((toId, fromId), distance))
      case (toId, ((fromId, distance), None)) => List(((toId, fromId), distance))
    }
  }.filter { case ((toId, fromId), _) => !toId.equals(fromId) } // Don't keep circular distances
}
