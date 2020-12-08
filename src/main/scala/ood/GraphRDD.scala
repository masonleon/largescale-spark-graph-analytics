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
//    val graph = context
    context
      .textFile(inputFile)
      .map { line =>
        val tokens = line.split(separator)
        (tokens(0), tokens(1))
      }
      .groupByKey()
      .cache()

//    graph
  }

  /**
   * Get number of edges. K = |V|.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @return k number of iterations for graph convergence.
   */
  def getNumEdges(GraphRDD: RDD[(String, Iterable[String])]): Int = {
//    val k = GraphRDD
    GraphRDD
      .count()
      .toInt

//    k
  }

  /**
   * Helper function to save output in coalesced single text file.
   *
   * @param GraphRDD representing graph G in adjacency list format as RDD[(V, List[(V)]).
   * @param outputFile representing string output file dir.
   */
  def saveSingleOutput(GraphRDD: RDD[(String, (String, Int))], outputFile: String) = {
    GraphRDD
      .coalesce(1)
      .saveAsTextFile(outputFile)
  }

  /**
   * adapted from 4.3.3. GEXF format for Gephi visualization software
   * https://livebook.manning.com/book/spark-graphx-in-action/chapter-4/ch04lev2sec6
   * https://livebook.manning.com/book/spark-graphx-in-action/chapter-4/point-9169-150-150-0
   * @param
   * @return
   */
  def getGexfRDD(context: SparkContext, GraphRDD: RDD[(String, Iterable[String])]): RDD[String] = {
    val xml =
      "<?xml " +
        "version=\"1.0\" " +
        "encoding=\"UTF-8\"" +
        "?>\n" +
        "<gexf " +
        "xmlns=\"http://www.gexf.net/1.2draft\" " +
        "version=\"1.2\"" +
        ">\n" +
        "  " +
        "<graph " +
        "mode=\"static\" " +
        "defaultedgetype=\"directed\"" +
        ">\n" +
        "    " +
        "<nodes>\n" +
        GraphRDD
          .map(v =>
            "     " +
              "<node " +
              "id=\"" + v._1 + "\" " +
              "label=\"" + v._1 + "\" " +
              "/>\n"
          ).collect.mkString +
        "    " +
        "</nodes>\n" +
        "    " +
        "<edges>\n" +
        GraphRDD
          .flatMap{ case (v, adjList) =>
            adjList
              .map(
                adjId =>
                  "      " +
                    "<edge " +
                    "source=\"" + v + "\" " +
                    "target=\"" + adjId + "\" " +
//                    "weight=\"" + edgeWeight + "\" " +
                    "/>\n"
              )
          }.collect.mkString +
        "    " +
        "</edges>\n" +
        "  " +
        "</graph>\n" +
        "</gexf>"

    context
      .parallelize(List(xml))
  }


  def saveGexfSingleOutput(GexfRDD: RDD[String], outputFile: String) = {
    GexfRDD
      .coalesce(1)
      .saveAsTextFile(outputFile)
  }
}
