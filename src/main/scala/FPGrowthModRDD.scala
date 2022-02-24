import Utils._
import java.{util => ju}
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec

object FPGrowthModRDD extends App {
  val sc = getSparkContext("FPGrowthModRDD")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val lines = getRDD(sc)
  val dataset = lines.map(x => x.split(spazioVirgola))

  val numParts = 10

  def getSingleItemCount(partitioner: Partitioner): Array[(String, Int)] = {
    dataset.flatMap(t => t).map(v => (v, 1))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minSupport)
      .collect()
      .sortBy(x => (-x._2, x._1))
  }

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction(lastNode: Node[Int], transazione: Array[Int]): Unit = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)
      //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
      addNodeTransaction(node._1, transazione.tail)
    }
  }

  def addToCondPattBase(transazione: Array[String], item: Map[String, Int]): Array[(Int, List[Int])] = {
    //Ordiniamo la transazione
    val filtered = transazione.flatMap(item.get)
    ju.Arrays.sort(filtered)
    val nuovaMappa = filtered.map(elem => elem -> filtered.slice(0, filtered.indexOf(elem)).toList)
    nuovaMappa
  }

  def exec(): Map[Set[String], Int] = {
    val partitioner = new HashPartitioner(numParts)

    //First step
    val singleItemsCount = getSingleItemCount(partitioner)
    println(singleItemsCount.mkString(","))
    val itemToRank = singleItemsCount.map(_._1).zipWithIndex.toMap

    val condPatternBase = dataset.flatMap(trans => addToCondPattBase(trans, itemToRank))

    val condPattGrouped = condPatternBase.groupByKey()

    val condPatternBaseFinal = condPattGrouped.map(elem => elem._1 -> elem._2.groupBy(el => el).map(e => (e._1, e._2.size)).toList)

    val freqItemSet = condPatternBaseFinal.flatMap(elem => itemSetFromOne(elem._1, elem._2, Map[Set[Int], Int]())).filter(_._2 >= minSupport)

    val indexToItem = itemToRank.map(x => x._2 -> x._1)
    val result = freqItemSet.collect().map(elem => (elem._1.map(indexToItem) -> elem._2)).toMap
    result
  }

  @tailrec
  def itemSetFromOne(item: Int, oneCondPatt: List[(List[Int], Int)], accSubMap: Map[Set[Int], Int]): Map[Set[Int], Int] = {
    if (oneCondPatt.nonEmpty) {
      val head = oneCondPatt.head
      val subMap = head._1.toSet.subsets().map(elem => elem + item -> head._2).filter(_._1.nonEmpty).toMap
      val subMapFinal = accSubMap ++ subMap.map { case (k, v) => k -> (v + accSubMap.getOrElse(k, 0)) }
      itemSetFromOne(item, oneCondPatt.tail, subMapFinal)
    } else {
      accSubMap
    }
  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthModRDDResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthModRDDResultSupport.txt")
}
