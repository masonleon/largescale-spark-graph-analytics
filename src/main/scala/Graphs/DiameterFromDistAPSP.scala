package Graphs

import ShortestPath.ShortestPaths.apspRDD
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.GraphRDD.{generateGraphRDD, saveSingleOutput}

object DiameterFromDistAPSP {
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nGraphDiameter <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Diameter")
//      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Input file: (userID, friendID)
    // Transform to...
    // Graph structure:  (userID, List[(friends)])
    // Graph structure is static --> persist or cache
    val graph = generateGraphRDD(sc, args(0), " ")
      .cache()

    val distances = apspRDD(graph)

    val diameter = getDiameter(sc, distances)

    saveSingleOutput(diameter, args(1) + "/diameter")
  }

  /**
   * Get graph diameter. The diameter is defined as the longest path in the set of all-pairs
   * shortest paths in the graph and is a common measure of network size. In a social network graph,
   * a small diameter would indicate a high degree of connectivity between members (no one person
   * has too many degrees of separation from another).
   *
   * @param context  representing SparkContext
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return diameter of graph.
   */
  def getDiameter(context: SparkContext, DistancesRDD: RDD[((String, String), Int)]) = {

    val diameter = DistancesRDD
      .sortBy(_._2, ascending = false)
      .take(1)

    context
      .parallelize(diameter)
  }
}
