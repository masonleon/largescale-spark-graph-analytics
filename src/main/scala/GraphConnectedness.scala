import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object GraphConnectedness {
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 1) {
      logger.error("Usage:\nGraphConnectedness <input>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("GraphConnectedness").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))

    val graph = data.map(line => line.split("\t"))
      .map(d => (d(0), d(1)))

    // use same partitioner to avoid reshuffling
    graph.persist()

    graph.saveAsTextFile("output")

    // implement DFS


  }
}
