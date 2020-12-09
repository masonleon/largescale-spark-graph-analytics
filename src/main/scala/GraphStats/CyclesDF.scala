package GraphStats

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CyclesDF {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    // set up local run arguments
    if (args.length != 2) {
      logger.error("Usage:\nTriangle.Rep_D <input dir> <output dir>")
      System.exit(1)
    }

    val sparkSession = SparkSession.builder
      .appName("CyclesDF").master("local")
      .getOrCreate()

    import sparkSession.implicits._

    val textFile = sparkSession.sparkContext.textFile(args(0))
    val user = textFile
      .map(line => (line.split(" ")(0), line.split(" ")(1)))
      .toDF("f", "t")

    var path = user.as("original").join(user.as("path"))
      .filter($"original.f" === $"path.t")
      .toDF("_a", "id3", "id1", "_b")
      .drop("_a", "_b")

    path.show()

    while(!path.head(1).isEmpty){
      path = path.as("inter").join(user.as("original"))
        .filter($"inter.id3" === "original.f")
        .toDF("_a", "id3", "id1", "_b")
        .drop("_a", "_b")

      path.show()
      System.exit(1)
    }


  }
}
