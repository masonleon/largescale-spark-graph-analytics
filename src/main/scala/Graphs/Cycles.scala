package Graphs

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Cycles {
  /**
   * Reads input from .tsv file.  Each line of input expected to be in the format "[fromID],
   * [toID]".  Searches for all cycles with the given k value
   *
   * @param args first argument: input file path, second argument: output file path
   */
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nCycles <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Cycles")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))

    val filter = 1000000
    val iterations = 5

    // "Hops" are the ways to get from a node to another node
    val hops = input
      .map { line =>
        val nodes = line.split("\t")
        (nodes(0), nodes(1))        // (fromId, toId)
      }
      .filter(x => x._1.toInt < filter && x._2.toInt < filter)

    val rev = hops.map{
      x => (x._2, x._1)
    }

    val complete = rev.leftOuterJoin(hops)
      .filter(x => x._2._2.nonEmpty)
      .map(node => (node._2._1, node._1))
      .distinct()
      .cache()

    // "Paths" are the ways to get to another node from some node, keeping track of nodes passed on the path
    var paths = hops.map { case (fromId, toId) => (toId, (fromId, List[String]())) }  // List will be intermediate nodes
    var pathSize = 1
    var maxCycleSize = 0L

    for(_ <- 1 to iterations) {
      pathSize += 1

      // get all paths of size "pathSize" starting at "from" ending at "to"
      paths =
        paths.join(complete)
          .filter { case (middleId, ((_, intermediates), _)) => !intermediates.contains(middleId) } // Don't join on a key where you've already been!
          .map { case (middleId, ((fromId, intermediates), toId)) =>
            (toId, (fromId, intermediates ++ List(middleId)))   // Add join key to list of intermediates
          }
          .distinct()

      // Count rows that are cycles
      val cycles = paths.filter { case (toId, (fromId, _)) => fromId.equals(toId) }
        .count()

      if (cycles > 0) {
        maxCycleSize = pathSize
      }

      // Only keep rows that are NOT cycles (to avoid endlessly going in circles)
      paths = paths.filter { case (toId, (fromId, _)) => !fromId.equals(toId) }
    }

    logger.info("\n\n\n!*!*!*!*!*!*!*!*!*!**!MaxCycleSize: " + maxCycleSize.toString + "\n\n\n")
    sc.parallelize(Seq(maxCycleSize))
      .coalesce(1)
      .saveAsTextFile(args(1))
  }
}
