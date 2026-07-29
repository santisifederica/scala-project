// Trova la coppia di celle con il maggior numero di date in comune: dedup delle celle per data
// dentro aggregateByKey, conteggio delle coppie separato dal recupero delle date della coppia vincente.
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

object CooccurrenceFinder {

  def generatePairs(cells: Array[Long]): Iterator[(Long, Long)] = {
    val sorted = cells.sorted
    for {
      i <- sorted.indices.iterator
      j <- (i + 1 until sorted.length).iterator
    } yield (sorted(i), sorted(j))
  }

  private def pairKey(a: Long, b: Long): (Long, Long) = (a, b)

  def findMaxCooccurrence(
    normalized: RDD[(Long, String)],
    numPartitions: Int
  ): ((Long, Long), List[String]) = {

    val cellsByDate: RDD[(String, Array[Long])] = normalized
      .map { case (cellCode, date) => (date, cellCode) }
      .aggregateByKey(mutable.HashSet.empty[Long], numPartitions)(
        (set, cell) => { set += cell; set },
        (s1, s2)    => { s1 ++= s2; s1 }
      )
      .filter { case (_, set) => set.size > 1 }
      .mapValues(_.toArray)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val pairCounts: RDD[((Long, Long), Int)] = cellsByDate
      .flatMap { case (_, cells) => generatePairs(cells).map(p => (p, 1)) }
      .reduceByKey(_ + _, numPartitions)

    val (winningPair, _) = pairCounts.reduce((a, b) => if (a._2 >= b._2) a else b)

    val (cellA, cellB) = winningPair
    val winningDates = cellsByDate
      .filter { case (_, cells) => cells.contains(cellA) && cells.contains(cellB) }
      .keys
      .collect()
      .sorted
      .toList

    cellsByDate.unpersist()

    (winningPair, winningDates)
  }
}