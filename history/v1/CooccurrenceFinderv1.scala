import org.apache.spark.rdd.RDD

object CooccurrenceFinderv1 {

  def generatePairs(cells: List[(Double, Double)]): List[((Double, Double), (Double, Double))] = {
    val pairs = scala.collection.mutable.ListBuffer[((Double, Double), (Double, Double))]()

    for (i <- cells.indices) {
      for (j <- i + 1 until cells.length) {
        val cell1 = cells(i)
        val cell2 = cells(j)

        val orderedPair = if (cell1._1 < cell2._1 || (cell1._1 == cell2._1 && cell1._2 < cell2._2)) {
          (cell1, cell2)
        } else {
          (cell2, cell1)
        }

        pairs += orderedPair
      }
    }

    pairs.toList
  }

  def findMaxCooccurrence(normalized: RDD[((Double, Double), String)]): (((Double, Double), (Double, Double)), List[String]) = {

    val byDate = normalized
      .map(x => (x._2, x._1))
      .groupByKey()

    val pairsByDate = byDate.flatMap(x => {
      val date = x._1
      val cells = x._2.toList
      val pairs = generatePairs(cells)
      pairs.map(pair => (pair, date))
    })

    val pairWithDates = pairsByDate
      .mapValues(date => List(date))
      .reduceByKey((dates1, dates2) => dates1 ++ dates2)
      .mapValues(dates => dates.sorted)

    val maxPair = pairWithDates
      .map(x => (x._2.length, (x._1, x._2)))
      .reduce((a, b) => if (a._1 > b._1) a else b)

    (maxPair._2._1, maxPair._2._2)
  }
}