import org.apache.spark.rdd.RDD
import Utils._
import scala.annotation.tailrec

object AprioriRDD extends App {
  val sc = getSparkContext("AprioriRDD")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = getRDD(sc)
  val items = dataset.flatMap(x => x.split(spazioVirgola))

  def gen(itemSets: RDD[Set[String]], size: Int): List[Set[String]] = {
    (itemSets reduce ((x, y) => x ++ y)).subsets(size).toList
  }

  def prune(itemCandRDD: RDD[Set[String]], itemsSetPrec: Array[Set[String]]) = {
    (itemCandRDD filter (x => x.subsets(x.size - 1) forall (y => itemsSetPrec.contains(y)))).collect()
  }

  def listOfPairs(str: String, itemSets: Array[Set[String]], size: Int) = {
    val items = str.split(",").toSet
    if (items.size >= size) {
      (itemSets.filter(y => y.subsetOf(items)))
    }
    else {
      Array[Set[String]]()
    }
  }

  //dafinire
  def countItemSet(itemSets: Array[Set[String]], size: Int) = {
    val ciao = dataset.flatMap(x => listOfPairs(x, itemSets, size))
    val itemPairs = ciao.map(x => (x, 1))
    val itemCounts = itemPairs.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= minSupport)
    itemCounts.collect().toMap
  }

  @tailrec
  def aprioriIter(mapItem: Map[Set[String], Int], dim: Int): Map[Set[String], Int] = {
    val itemsSetPrec = sc.parallelize(mapItem.keys.filter(x => x.size == (dim - 1)).toList) //vedere se cambia qualcosa con persist o meno
    val candidati = gen(itemsSetPrec, dim)
    val itemSets = prune(sc.parallelize(candidati), itemsSetPrec.collect())
    val itemSetCounts = countItemSet(itemSets, dim).filter(_._2 >= minSupport)

    if (itemSetCounts.isEmpty) {
      mapItem
    }
    else {
      aprioriIter(mapItem ++ itemSetCounts, dim + 1)
    }
  }

  def firstStep(): Map[Set[String], Int] = {
    val itemPairs = items.map(x => (Set(x), 1))
    val itemCounts = itemPairs.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= minSupport)
    itemCounts.collect().toMap
  }

  def exec() = {
    val k = firstStep()

    if (k.isEmpty) {
      k
    }
    else {
      aprioriIter(k, 2)
    }
  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "AprioriRDDResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "AprioriRDDResultSupport.txt")
}