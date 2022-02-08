import Utils.{minSupport, time}

import java.{util => ju}
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.mutable

object FPGrowthRDDSenzaAlbero extends App {
  val sc = Utils.getSparkContext("FpGrowthRDD")
  val lines = Utils.getRDD("Market_Basket_Optimisation.csv", sc)
  val dataset = lines.map(x => x.split(","))
  val dataset2 = sc.parallelize(
    List(Array("a", "c", "d", "f", "g", "i", "m", "p")
      , Array("a", "b", "c", "f", "i", "m", "o")
      , Array("b", "f", "h", "j", "o")
      , Array("b", "c", "k", "s", "p")
      , Array("a", "c", "e", "f", "l", "m", "n", "p")))

  val numParts = 10

  def getSingleItemCount(partitioner: Partitioner): Array[(String, Int)] = {
    dataset2.flatMap(t => t).map(v => (v, 1))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= Utils.minSupport)
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

  def printTree(tree: Node[Int], items: Map[Int, String], str: String): Unit = {
    if (tree.occurrence != -1) {
      println(str + items(tree.value) + " " + tree.occurrence)
      tree.sons.foreach(printTree(_, items, str + "\t"))
    }
    else {
      tree.sons.foreach(printTree(_, items, str))
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

    val condPatternBase = dataset2.flatMap(trans => addToCondPattBase(trans, itemToRank))

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

  val result = Utils.time(exec())
  val numTransazioni = dataset2.count().toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthFreqItemSetRDDBasketOptim.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthAssRulestRDDBasketOptim.txt")
}
