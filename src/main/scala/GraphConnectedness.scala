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
    val data = sc.textFile(args(0)).map(x => x.split(" "))

    val graph = data.map(d => (d(0), d(1)))
      .groupByKey()
      .persist()

    //TODO use same partitioner to avoid reshuffling

    // initialize all edges not visited to 0
    val nodes = graph.flatMap({
        case (node, adjList) => adjList.map(adjID => (node, (adjID, 0)))
      }
    )

    // implement BFS/DFS to traverse all connected users


    nodes.saveAsTextFile("GraphConnectednessOutput")
  }
}
