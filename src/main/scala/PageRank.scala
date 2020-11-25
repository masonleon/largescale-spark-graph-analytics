import org.apache.log4j.LogManager
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {
  val alpha = 0.15
  private var lostMass = 0d

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nPageRank <k value> <num iterations> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    val k = args(0).toInt   // TODO set as a global value?
    val iterations = args(1).toInt
    val numVertices = (k * k).toDouble  // TODO would be nice if this was global as well?

    val initialPR = 1d
    val realRanks = for (i <- 1 to k * k) yield (i, initialPR)
    val dummyRank = List((0, 0d))
    var ranks = sc.parallelize(realRanks ++ dummyRank)
    val rankPartitioner = ranks.partitioner match {   // Get partitioner to use with graphs and contributions
      case Some(p) => p
      case None => new HashPartitioner(ranks.partitions.length)
    }

    val links = for (i <- 1 until k * k by k; j <- i until i + k - 1) yield (j, j + 1)
    val danglingPages = for (i <- k to k * k by k) yield (i, 0)
    val graph = sc.parallelize(links ++ danglingPages)
      // For our graph, there won't actually be more than one instance of each key,
      // but for a graph that is more complex there might be
      .groupByKey(rankPartitioner) // Partition with rank partitioner to reduce join cost
      .cache() // Keep this around bc it is static
//      .persist(StorageLevel.MEMORY_ONLY_SER)  // Could do this instead of cache if graph doesn't fit in memory - it would be more space efficient but more computationally expensive

    for (i <- 1 to iterations) {
      // Get the incoming contributions for each page
      val contribs = graph.join(ranks).values
        .flatMap { case (pages, rank) =>
          val size = pages.size   // For our graph, size will always = 1
          pages.map(pg => (pg, rank / size))
      }
        .reduceByKey(rankPartitioner, _ + _)  // Sum up contributions to each page, partitioning with rank partitioner to reduce future join cost

      lostMass = contribs.lookup(0).sum  // Get the mass lost to dummy page

      ranks = ranks.leftOuterJoin(contribs).map {   // Calculate the new rank for each page
        case (page, (_, Some(sumContribs))) => (page, calculateRank(sumContribs, numVertices))
        case (page, (_, None)) => (page, calculateRank(0, numVertices))
      }
    }

//    logger.info("^#^#^#^#^ ranks before final filter: " + ranks.toDebugString)

    val finalRanks = ranks
      .map { case (page, rank) =>
        if (page == 0) {
          (page, 0d)
        } else {
          (page, rank)
        }
      }
      .sortByKey()

    logger.info("!*!*!*!**! total PR mass: " + finalRanks.values.sum)

    finalRanks.saveAsTextFile(args(2))
  }

  private def calculateRank(sumContribs: Double, numVertices: Double) = {
    alpha + ((1 - alpha) * (sumContribs + (lostMass / numVertices)))
  }
}