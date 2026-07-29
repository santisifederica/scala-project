// Normalizza le righe del CSV in coppie (cellCode, date): arrotonda le coordinate alla cella
// geografica e le impacchetta in una chiave Long, estrae la data ed effettua il parsing difensivo.
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Normalizer {

  private val OFFSET = 2000L

  def encodeCell(latTenths: Long, lonTenths: Long): Long =
    (latTenths + OFFSET) * 100000L + (lonTenths + OFFSET)

  def decodeCell(code: Long): (Double, Double) = {
    val lonTenths = code % 100000L - OFFSET
    val latTenths = code / 100000L - OFFSET
    (latTenths / 10.0, lonTenths / 10.0)
  }

  private def roundToTenth(value: Double): Long =
    Math.round(value * 10)

  private def parseRow(row: Row): Option[(Long, String)] = {
    try {
      val lat = row.getAs[String]("latitude").trim.toDouble
      val lon = row.getAs[String]("longitude").trim.toDouble
      val date = row.getAs[String]("date").trim.split(" ")(0)

      val cellCode = encodeCell(roundToTenth(lat), roundToTenth(lon))
      Some((cellCode, date))
    } catch {
      case _: Exception => None
    }
  }

  def normalize(data: RDD[Row]): RDD[(Long, String)] =
    data.flatMap(parseRow)
}