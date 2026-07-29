// Entry point: legge input/output/numPartitions da riga di comando ed esegue la pipeline Spark.
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter

object Earthquake {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: Earthquake <inputPath> <outputPath> [numPartitions]")
      sys.exit(1)
    }

    val inputPath     = args(0)
    val outputPath    = args(1)
    val numPartitions = if (args.length > 2) args(2).toInt else 16

    val conf = new org.apache.spark.SparkConf().setAppName("Earthquake Cooccurrence")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println(s"[INFO] numPartitions=$numPartitions, defaultParallelism=${spark.sparkContext.defaultParallelism}")

    val startTime = System.currentTimeMillis()

    val rawData = spark.read
      .option("header", value = true)
      .csv(inputPath)
      .repartition(numPartitions)
      .rdd

    val normalized = Normalizer.normalize(rawData)

    val (winningPairCode, dates) = CooccurrenceFinder.findMaxCooccurrence(normalized, numPartitions)

    val (cellA, cellB) = (Normalizer.decodeCell(winningPairCode._1), Normalizer.decodeCell(winningPairCode._2))

    val elapsed = System.currentTimeMillis() - startTime
    println(s"[INFO] Tempo di esecuzione: ${elapsed}ms")

    val resultLine = s"($cellA, $cellB)"

    val allLines = resultLine +: dates

    if (outputPath.startsWith("gs://")) {
      val fs = FileSystem.get(new java.net.URI(outputPath), spark.sparkContext.hadoopConfiguration)
      val writer = new PrintWriter(fs.create(new Path(outputPath)))
      try {
        allLines.foreach(writer.println)
      } finally {
        writer.close()
      }
    } else {
      val outFile = new java.io.File(outputPath)
      Option(outFile.getParentFile).foreach(_.mkdirs())
      val writer = new PrintWriter(outFile)
      try {
        allLines.foreach(writer.println)
      } finally {
        writer.close()
      }
    }

    allLines.foreach(println)

    spark.stop()
  }
}