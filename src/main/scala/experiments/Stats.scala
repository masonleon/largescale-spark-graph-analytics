package experiments

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Stats {

  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nGexfConvert <input> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("Stats")

    val sc = new SparkContext(conf)

    // entry point to DSET and DFRAME API
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // ***** numTotalEdges *************************************************************************
    val numTotalEdges = sc
      .textFile(args(0))
      .count()

    sc
      .parallelize(List(numTotalEdges))
      .coalesce(1)
      .saveAsTextFile(args(1) + "/stats/numTotalEdges")
    // *********************************************************************************************

    // ***** countsNodeInEdges *********************************************************************
    val countsNodeInEdges = sc
      .textFile(args(0))
      .flatMap(edge => edge.split(" "))
      .map{user => (user.split("\t")(1).toInt, 1)}
      .reduceByKey(_ + _)
      .sortByKey(true)
//      .cache()

    countsNodeInEdges
      .coalesce(1)
      .toDF
      .write
      .csv(args(1) + "/stats/countsNodeInEdges")
    // *********************************************************************************************

    // ***** numNodesWithInEdges *******************************************************************
    val numNodesWithInEdges = countsNodeInEdges
      .count()

    sc
      .parallelize(List(numNodesWithInEdges))
      .coalesce(1)
      .saveAsTextFile(args(1) + "/stats/numNodesWithInEdges")
    // *********************************************************************************************

    // ***** countsNodeOutEdges ********************************************************************
    val countsNodeOutEdges = sc
      .textFile(args(0))
      .flatMap(edge => edge.split(" "))
      .map(user => (user.split("\t")(0).toInt, 1))
      .reduceByKey(_ + _)
      .sortByKey(true)
//      .cache()

    countsNodeOutEdges
      .coalesce(1)
      .toDF
      .write
      .csv(args(1) + "/stats/countsNodeOutEdges")
    // *********************************************************************************************

    // ***** numNodesWithOutEdges ******************************************************************
        val numNodesWithOutEdges = countsNodeOutEdges
      .count()

    sc
      .parallelize(List(numNodesWithOutEdges))
      .coalesce(1)
      .saveAsTextFile(args(1) + "/stats/numNodesWithOutEdges")
    // *********************************************************************************************

    // ***** nodeStatsDF ***************************************************************************
    val NodeInEdgeCountDF = countsNodeInEdges
//      .coalesce(1)
      .toDF("id", "in_edges_(num_followers)")

    val NodeOutEdgeCountDF = countsNodeOutEdges
//      .coalesce(1)
      .toDF("id", "out_edges_(num_following)")

    val nodeStatsDF = NodeInEdgeCountDF
      .join(
        NodeOutEdgeCountDF,
        Seq("id"),
//        NodeInEdgeCountDF("id")===NodeOutEdgeCountDF("id"),
        "fullouter"
      )
      .na
      .fill(0)
      .sort("id")

    nodeStatsDF
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(args(1) + "/stats/nodeStatsDf")
    // *********************************************************************************************

  }
}
