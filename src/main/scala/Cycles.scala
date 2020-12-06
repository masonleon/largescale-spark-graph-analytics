import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object Cycles {
  /**
    * Reads input from .csv file.  Each line of input expected to be in the format "[fromID],
    * [toID]".  Identifies the largest cycle between users.
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

    // Line of input: (fromId, toId)
    // data = sc.textFile(input).map(line => (toId, fromId))
    // path = data.map((toId, fromId) => (fromId, toId))
    // pathSize = 1
    // maxCycleSize = 0
    //  while path is not empty:			// iterate until path no longer grows
    //    pathSize += 1				// Path length grows with each iteration
    //    path = path.join(data)		// Find next “hops” in path
    //    cycle = path.filter(matching keys)		// Keep only closed cycles
    //    // filter out keys from path that are in cycle to keep from going in circles
    //    path = path.leftJoin(cycle).filter(Keep rows that were NOT in cycles)
    //    if cycle:
    //      maxCycleSize = pathSize

    val hops = input
      .map { line =>
        val nodes = line.split(" ")
        (nodes(0), nodes(1))        // (fromId, toId)
      } //TODO persist/cache? b/c it is static?

    var paths = hops.map { case (fromId, toId) => (toId, fromId) }
    var pathSize = 1
    var maxCycleSize = 0L

    while (!paths.isEmpty()) {
      pathSize += 1

      // get all paths of size "pathSize" starting at "from" ending at "to"
      paths = paths.join(hops)
        .map { case (_, (fromId, toId)) => (toId, fromId) }
        .distinct()

      // Count rows that are cycles
      val cycles = paths.filter { case (toId, fromId) => fromId.equals(toId) }
        .count()

      if (cycles > 0) {
        maxCycleSize = pathSize
      }

      // Only keep rows that are NOT cycles (to avoid endlessly going in circles)
      paths = paths.filter { case (toId, fromId) => !fromId.equals(toId) }
    }

    logger.info("\n\n\n!*!*!*!*!*!*!*!*!*!**!MaxCycleSize: " + maxCycleSize.toString + "\n\n\n")
    sc.parallelize(Seq(maxCycleSize))
      .coalesce(1)
      .saveAsTextFile(args(1))
  }
}
