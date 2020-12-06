package ood

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
   * @return cached graph G in adjacency list format as RDD[(V, List[(V)])
   */
  def generateGraphRDD(context: SparkContext, inputFile: String, separator: String): RDD[(String, Iterable[String])] = {
    val graph = context
      .textFile(inputFile)
      .map { line =>
        val tokens = line.split(separator)
        (tokens(0), tokens(1))
      }
      .groupByKey()
      .cache()

    graph
  }

}
