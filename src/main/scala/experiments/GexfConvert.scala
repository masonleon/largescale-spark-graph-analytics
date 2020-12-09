package experiments

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import utils.GexfRDD.{getGexfRDD, saveGexfSingleOutput}
import utils.GraphRDD.{generateMaxFilteredGraphRDD}

object GexfConvert {

  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nGexfConvert <input> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("GexfConvert")

    val sc = new SparkContext(conf)

    val graph = generateMaxFilteredGraphRDD(sc, args(0), "\t", 100)
      .cache()

    val gexf = getGexfRDD(sc, graph)

    saveGexfSingleOutput(gexf, args(1) + "/gexf")
  }
}
