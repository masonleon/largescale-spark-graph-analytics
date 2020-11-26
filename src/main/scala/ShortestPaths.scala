import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object ShortestPaths {

  /**
    * Number of iterations.
    */
  val k: Int = 2      // TODO is there a better way to make sure the graph converges other than running |V| iterations?

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nPageRank <k value> <num iterations> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("PageRank")
    val sc = new SparkContext(conf)


    // Input file: (departID, arrivalID, distance)
    // Transform to...
    // Graph structure:  (departID, List[(arrivalID, weight)])
    // Graph structure is static --> persist or cache
    val graph = sc.textFile(args(0))
      .map { line =>
        val tokens = line.split(",") //TODO confirm file format as csv?
      val departID = tokens(0)
        val arrivalID = tokens(1)
        val weight = tokens(2).toInt
        (departID, arrivalID, weight)
      } // If there are multiple flights for same route, they *should be* same distance (although I'm not sure that they always will be in the input data)
      .groupBy { case (dep, arr, _) => (dep, arr) } // Get all flights with same dep/arr together
      .mapValues(flights => flights(0)) // Take one representative flight for each
      .map { case ((dep, arr), wt: Int) =>
      (dep, (arr, wt))
    }
      .groupByKey()
      .cache()

    // Distances structure: (aiportID, totalDistance to get here)
    // This data will change each iteration
    // Initially the totalDistance will be the minimum distance from any starting airport
    var distances = graph.values
      .flatMap(flight => flight)
      .groupByKey()
      .mapValues(distances => distances.min)

    // How to calculate the shortest path?
    // All departure airports are in the graph.  If there exists a shortest path to anywhere, it must start at a dep airport in the graph
    // On a shortest path, for all nodes in the route, the path is also the shortest path for those nodes
    // So we really just need to build up the routes from every possible starting location?
    // And then after convergence go through and output the final results?  How?
    // Will every node get covered?
    // How could we keep track of the route for each path?

    for (iteration <- 0 to k) {
      val temp = graph.join(distances)    // (airportID, ajList, totalDistance)
        .flatMap{ case (id, (adjFlights, totalDistance)) => updateDistances(id, adjFlights, totalDistance)}
        .reduceByKey((x, y) => Math.min(x, y))    // Only keep min distance for any airport
    }
  }

  /**
    * For all flights departing from origin airport to arrival airport, return arrival ID along with cumulative distance from origin airport.
    * Also return origin ID with its original distance to maintain this data in the distances dataset.
    * @param originId ID of origin airport
    * @param adjFlights all (arrival ID, weight) for flights departing from origin airport
    * @param distance cumulative distance to origin airport
    * @return iterable of (airportID, cumulativeDistance) including origin and all its adjacent airports
    */
  def updateDistances(originId: String, adjFlights: Iterable[(String, Int)], distance: Int): Iterable[(String, Int)] = {
    adjFlights.map { case (id, wt) => (id, wt + distance)} ++ List((originId, distance))
  }
}
