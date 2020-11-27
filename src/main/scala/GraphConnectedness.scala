import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object GraphConnectedness {
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nGraphConnectedness <input> <output>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("GraphConnectedness")
    val sc = new SparkContext(conf)

    // implement DFS
    

  }
}
