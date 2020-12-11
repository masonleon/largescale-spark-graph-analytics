package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GraphRDD {

  /**
   * Generate a graph G of pair (V, E) in adjacency list format as RDD, where V is set of vertices and E is
   * set of edges, such that E ⊆ V x V. Graph generated from input text file representing |E| where
   * each edge is record in text file of v1 -> v2.
   *
   * @param context representing SparkContext
   * @param inputFile representing argument for input file
   * @param separator representing the separator character such as ",", " ", "|", etc.*
   * @return graph G in adjacency list format as RDD[(V, List[(V)])
   */
  def generateGraphRDD(context: SparkContext, inputFile: String, separator: String): RDD[(String, Iterable[String])] = {
    context
      .textFile(inputFile)
      .map { line =>
        val tokens = line.split(separator)
        (tokens(0), tokens(1))
      }
      .groupByKey()
//      .cache()
  }

  /**
   * Generate a max filtered graph G of pair (V, E) in adjacency list format as RDD, where V is set of vertices and E is
   * set of edges, such that E ⊆ V x V. Graph generated from input text file representing |E| where
   * each edge is record in text file of v1 -> v2.
   *
   * @param context representing SparkContext
   * @param inputFile representing argument for input file
   * @param separator representing the separator character such as ",", " ", "|", etc.*
   * @return graph G in adjacency list format as RDD[(V, List[(V)])
   */
  def generateMaxFilteredGraphRDD(context: SparkContext, inputFile: String, separator: String, threshold: Int): RDD[(String, Iterable[String])] = {
    context
      .textFile(inputFile)
      .map { line =>
        val tokens = line.split(separator)
        (tokens(0), tokens(1))
      }
      // maxfilter to reduce datasize input
      .filter(user => {
        user._1.toInt <= threshold && user._2.toInt <= threshold
      })
      .groupByKey()
    //      .cache()
  }

  /**
   * Get number of edges. K = |V|.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return k number of iterations for graph convergence.
   */
  def getNumEdges(GraphRDD: RDD[(String, Iterable[String])]): Int = {
    GraphRDD
      .count()
      .toInt
  }

  /**
   * Helper function to save output in coalesced single text file.
   *
   * @param data any RDD containing key (String, String) and value (Int)
   * @param outputFile representing string output file dir.
   */
  def saveSingleOutput(data: RDD[((String, String), Int)], outputFile: String) = {
    data
      .coalesce(1)
      .saveAsTextFile(outputFile)
  }
}
