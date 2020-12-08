import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphStats {
  def main(args: Array[String]): Unit = {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 1) {
      logger.error("Usage:\nGraphConnectedness <input>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("GraphConnectedness")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val data = sc
      .textFile(args(0))
      .map(x => x.split(" "))

    val graph = data
      .map(d => (d(0), d(1)))
      .groupByKey()
      .persist()

    //TODO use same partitioner to avoid reshuffling

    // starting node
    var active:RDD[(String, Int)] = sc.parallelize(Seq(("0",1)))

    // implement BFS to traverse all connected users
    for(iteration <- 1 to 4){
      active = graph
        .join(active)
        .flatMap({ case (id, (adjList, dummy)) => adjList.map(x => (x, 1))
        })
        .reduceByKey((x,y) => x)
    }

    /**
     * Pseudo-Code for Diameter + Largest Cycle
     * Diameter -- takes all-pairs solution from ShortestPaths.scala and finds the path with max distance --
     *
     * Largest Cycle
     * iterative solution similar to triangles to start
     * var path = data.join(reverse_data)
     * var cycle = path.filter(matching keys) // iter1
     * continue iterations until path no longer grows
     */

    active
      .saveAsTextFile("GraphConnectednessOutput")
  }
}