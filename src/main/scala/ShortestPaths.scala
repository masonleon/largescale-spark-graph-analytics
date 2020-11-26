import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPaths {

  /**
    * A path from original departure airport to final arrival airport.
    * @param start airport ID for original departure airport for this path
    * @param end airport ID for final arrival airport for this path
    */
  // I envision using this as a key.  We will need a Path for every combination of airports.
  // Two path keys would be equivalent if k1.start == k2.start AND k1.end == k2.end
  // TODO how to codify equivalence?  Is there like an equals method for case classes?
  case class Path(start: String, end: String)

  /**
    * A complete route for a given path from original arrival airport to final destination airport.
    * @param path Path indicating original departure and final arrival airport IDs
    * @param totalDistance cumulative distance for this route so far
    * @param intermediates ordered list of all intermediate airport IDs for this route so far
    * @param isComplete true if the route is complete from departure to arrival; false otherwise
    */
  // Each Path would use a Route to build up the shortest path
  case class Route(path: Path, totalDistance: Int, intermediates: List[String], isComplete: Boolean)


  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nPageRank <k value> <num iterations> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)

    // Graph structure:  (departID, List[(arrivalID, distance)])
    // Graph structure is static --> persist or chache
    // Input file: (departID, arrivalID, distance)
    val graph = sc.textFile(args(0))
      .map { line =>
        val tokens = line.split(",")  //TODO confirm file format as csv?
        val departID = tokens(0)
        val arrivalID = tokens(1)
        val distance = tokens(2).toInt
        (departID, arrivalID, distance)
      }
      .groupBy{ case (dep,arr, _) => (dep, arr)}    // Get all flights with same dep/arr together
      .mapValues(flights => flights(0))             // Take one representative flight for each
      .map { case ((dep, arr), dist: Int) =>
        (dep, (arr, dist))
    }
      .groupByKey()
  }


}
