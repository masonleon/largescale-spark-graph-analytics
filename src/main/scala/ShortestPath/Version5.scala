package ShortestPath
import PathUtils.{generateGraphRDD, updateDistances}
import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Version5 {
  /**
    * Weight for each edge connecting vertices in the graph.
    */
  val edgeWeight = 1

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nShortestPathsV4 <input> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("ShortestPathsV4")
    val sc = new SparkContext(conf)

    // Graph structure:  (userID, List[(friends)])
    val graph = generateGraphRDD(sc, args(0), "\t")
    val graphPartitioner = graph.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(graph.partitions.length)
    }

    // Distances structure: (toId, (fromId, distance))
    // This data will change each iteration
    var distances = graph.flatMap { case (fromId, adjList) =>
      adjList.map(adjId => (adjId, (fromId, edgeWeight)))
    }
      .partitionBy(graphPartitioner)

    val iterCount = sc.accumulator(0)
    var numUpdated = 1L
    while (numUpdated > 0) {
      val temp = distances.leftOuterJoin(graph)   // (toId, ((fromId, distance), Option[adjList]))
        .flatMap(x => updateDistances(x, edgeWeight))
        .reduceByKey((x, y) => Math.min(x, y)) // Only keep min distance for any (to, from) pair

      // Count the number of new paths found
      numUpdated = temp.count() - distances.count()

      distances = temp.map(x => (x._1._1, (x._1._2, x._2)))

      iterCount += 1
    }

    logger.info("\n\n\n!*!*!*!*!*!*Number of iterations: " + iterCount.value.toString + "\n\n\n")
    distances.saveAsTextFile(args(1) + "/convergedDistances")

    sc.parallelize(Seq(iterCount.value)).coalesce(1).saveAsTextFile(args(1) + "/diameter")
  }
}
