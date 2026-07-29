import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Normalizer {

  private def roundCoord(value: Double): Double =
    Math.round(value*10).toDouble/10

  private def extractDate(timestamp: String): String =
    timestamp.trim.split(" ")(0)

  def normalize(data: RDD[Row]): RDD[((Double, Double), String)] = {
    data.map(
      row => {
        val normLatitude = roundCoord(row.getAs[String]("latitude").toDouble)
        val normLongitude = roundCoord((row.getAs[String]("longitude").toDouble))
        val normDate = extractDate(row.getAs[String]("date"))
        ((normLatitude, normLongitude),normDate)
      }
    ).distinct()
  }
}