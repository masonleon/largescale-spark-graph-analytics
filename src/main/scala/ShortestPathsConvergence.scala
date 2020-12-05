import ShortestPaths.{generateGraphRDD, initializeDistances, saveSingleOutput, updateDistances}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPathsConvergence {

  /**
    * Weight for each edge connecting vertices in the graph.
    */
  val edgeWeight = 1
  val optimizeJoin = true

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nShortestPathsConvergence <input> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("ShortestPaths")
    val sc = new SparkContext(conf)

    // Graph structure:  (userID, List[(friends)])
    val graph = generateGraphRDD(sc, args(0), " ")

    // Distances structure: ((toId, fromId), distance)
    // This data will change each iteration
    var distances = initializeDistances(graph, optimizeJoin)

    var iterCount = 0
    var numUpdated = 1L
    while (numUpdated > 0) {
      val temp = distances.map { case ((toId, fromId), distance) => (toId, (fromId, distance)) }
        .leftOuterJoin(graph)   // (toId, ((fromId, distance), Option[adjList]))
        .flatMap(x => updateDistances(x))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair

      numUpdated = temp.leftOuterJoin(distances)  // ((to, from), newDist, Option[oldDist])
        .filter {
        case (_, (newDist, Some(oldDist))) => newDist != oldDist
        case (_, (_, None)) => true
      }.count()

      distances = temp
      iterCount += 1
    }

    logger.info("!*!*!*Number of iterations: " + iterCount.toString)
    saveSingleOutput(distances, args(1) + "/convergedDistances")
  }
}
