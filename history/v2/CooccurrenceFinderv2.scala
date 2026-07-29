/* Ottimizzazioni applicate:
    - cache() : evito che l'RDD normalizzato venga ricalcolato più volte
    - aggregateByKey() con Set: sostituisco groupByKey(), aggrego già dentro la partizione usando un Set che
    garantisce anche l'unicità delle celle per data
    - aggregateByKey con ListBuffer: più efficiente di mapValues + reduceByKey con concatenazione di liste
    - treeReduce: il reduce finale avviene in modo gerarchico tra i nodi invece che su uno solo
    - aggregateByKey: la deduplicazione delle celle per data avviene direttamente durante l'aggregazione
*/


import org.apache.spark.rdd.RDD

object CooccurrenceFinderv2 {

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

    normalized.cache()

    val byDate = normalized
      .map(x => (x._2, x._1))
      .aggregateByKey(scala.collection.mutable.Set[(Double, Double)]())(
        (set, cell) => { set += cell; set },
        (set1, set2) => { set1 ++= set2; set1 }
      )

    val pairsByDate = byDate.flatMap(x => {
      val date = x._1
      val cells = x._2.toList
      val pairs = generatePairs(cells)
      pairs.map(pair => (pair, date))
    })

    val pairWithDates = pairsByDate
      .aggregateByKey(scala.collection.mutable.ListBuffer[String]())(
        (buf, date) => { buf += date; buf },
        (buf1, buf2) => { buf1 ++= buf2; buf1 }
      )
      .mapValues(buf => buf.toList.sorted)

    val maxPair = pairWithDates
      .map(x => (x._2.length, (x._1, x._2)))
      .treeReduce((a, b) => if (a._1 > b._1) a else b)

    (maxPair._2._1, maxPair._2._2)
  }
}