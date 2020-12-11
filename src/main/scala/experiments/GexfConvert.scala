package experiments

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import utils.GraphRDD.{generateGraphRDD,generateMaxFilteredGraphRDD}
import org.apache.spark.sql.SparkSession

object GexfConvert {

  val threshold = 10000
  val separator = "\t"

  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nGexfConvert <input> <output dir>")
      System.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1) + "/gexf"

    val conf = new SparkConf()
      .setAppName("GexfConvert")

    val sc = new SparkContext(conf)

    // entry point to DSET and DFRAME API
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val edgesDF = sc
      .textFile(inputFile)

    val countsNodeInEdges = edgesDF
      .flatMap(edge => edge.split(" "))
      .map(edge => (edge.split(separator)(0), edge.split(separator)(1)))
      // maxfilter to reduce datasize input
      .filter(user => {user._1.toInt <= threshold && user._2.toInt <= threshold})
//      .map{user => (user.split(separator)(1).toInt, 1)}
      .map(user => (user._1, 1))
      .reduceByKey(_ + _)
      .sortByKey(true)
      .cache()

    val countsNodeOutEdges = edgesDF
      .flatMap(edge => edge.split(" "))
//      .map{user => (user.split(separator)(0).toInt, 1)}
      .map(edge => (edge.split(separator)(0), edge.split(separator)(1)))
      // maxfilter to reduce datasize input
      .filter(user => {user._1.toInt <= threshold && user._2.toInt <= threshold})
      .map(user => (user._2, 1))
      .reduceByKey(_ + _)
      .sortByKey(true)
      .cache()

    val NodeInEdgeCountDF = countsNodeInEdges
      .toDF("id", "in_edges")
      .cache()

    val NodeOutEdgeCountDF = countsNodeOutEdges
      .toDF("id", "out_edges")
      .cache()

    val nodeStatsDF = NodeInEdgeCountDF
      .join(
        NodeOutEdgeCountDF,
        Seq("id"),
        "fullouter"
      )
      .na
      .fill(0)
      .sort("id")
//      .coalesce(1)
      .cache()

//    val graph =
//      generateGraphRDD(sc, inputFile, separator)
////    generateMaxFilteredGraphRDD(sc, inputFile, separator, threshold)
//      .cache()

    val xml =
//      "<?xml " +
//        "version=\"1.0\" " +
//        "encoding=\"UTF-8\"" +
//      "?>\n" +
//      "<gexf " +
//        "xmlns=\"http://www.gexf.net/1.2draft\" " +
//        "version=\"1.2\"" +
//      ">\n" +
      "  " +
//        "<graph " +
//          "mode=\"static\" " +
//          "defaultedgetype=\"directed\"" +
//        ">\n" +
      "    " +
          "<nodes>\n" +
            nodeStatsDF
              .map(row =>
      "     " +
            "<node " +
              "id=\"" + row.getAs("id") + "\" " +
              "label=\"" + row.getAs("id") + "\" " +
              "in_edges=\"" + row.getAs("in_edges") + "\" " +
              "out_edges=\"" + row.getAs("out_edges") + "\" " +
            "/>\n"
              )
              .collect
              .mkString +
      "    " +
          "</nodes>\n" +
      "    " +
//        "<edges>\n" +
//            graph
//              .flatMap{ case (v, adjList) => adjList
//                .map(adjId => (v.toInt,
//      "      " +
//          "<edge " +
//            "source=\"" + v + "\" " +
//            "target=\"" + adjId + "\" " +
//          "/>\n")
//                )
//              }
//              .sortByKey(true)
//              .values
//              .collect
//              .mkString +
//      "    " +
//        "</edges>\n" +
      "  "
//    +
//      "</graph>\n" +
//      "</gexf>"

    sc
      .parallelize(List(xml))
      .coalesce(1)
      .saveAsTextFile(outputFile)
  }

}
