package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object GexfRDD {

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
//            GraphRDD
//              .map(v =>
//                (v._1.toInt,
//                "     " +
//                  "<node " +
//                    "id=\"" + v._1 + "\" " +
//                    "label=\"" + v._1 + "\" " +
//                    "in_edges=\"" + v._2 + "\" " +
//                    "out_edges=\"" + v._3 + "\" " +
//                  "/>\n"
//                )
//              )
//              .sortByKey(true)
//              .values
//              .collect
//              .mkString +
      "    " +
          "</nodes>\n" +
      "    " +
          "<edges>\n" +
            GraphRDD
              .flatMap{ case (v, adjList) =>
                adjList
                  .map(adjId =>
                    (v.toInt,
                    "      " +
                      "<edge " +
                        "source=\"" + v + "\" " +
                        "target=\"" + adjId + "\" " +
                      "/>\n"
                    )
                  )
              }
              .sortByKey(true)
              .values
              .collect
              .mkString +
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
