package apriori

import mainClass.MainClass.minSupport
import org.apache.spark.rdd.RDD
import utils.Utils._

import scala.annotation.tailrec

object AprioriRDD extends App {

  def exec() = {
    val sc = getSparkContext("AprioriRDD")
    //Prendiamo il dataset (vedi Utils per dettagli)
    val (dataset, dimDataset) = getRDD(sc)
    val items = dataset.flatMap(x => x.split(" "))

    def generazioneCandidati(itemSets: RDD[Set[String]], size: Int): List[Set[String]] = {
      (itemSets reduce ((x, y) => x ++ y)).subsets(size).toList
    }

    def prune(itemCandRDD: List[Set[String]], itemsSetPrec: Array[Set[String]]) = {
      if (itemCandRDD.head.size > 2) {
        itemCandRDD.filter(x => x.subsets(x.size - 1).forall(y => itemsSetPrec.contains(y)))
      } else itemCandRDD
    }

    def listOfTuples(str: String, itemSets: List[Set[String]], size: Int) = {
      val items = str.split(" ").toSet
      if (items.size >= size) {
        itemSets.filter(y => y.subsetOf(items))
      }
      else {
        List[Set[String]]()
      }
    }

    //
    def countItemSet(itemSets: List[Set[String]], size: Int) = {
      val listAllCandidates = dataset.flatMap(x => listOfTuples(x, itemSets, size))
      val itemSetCount = listAllCandidates.map(x => (x, 1))
      val itemCounts = itemSetCount.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= minSupport)
      itemCounts.collect().toMap
    }

    @tailrec
    def aprioriIter(mapItem: Map[Set[String], Int], dim: Int): Map[Set[String], Int] = {
      val itemsSetPrec = sc.parallelize(mapItem.keys.filter(x => x.size == (dim - 1)).toList)
      val candidati = generazioneCandidati(itemsSetPrec, dim)
      val itemSets = prune(candidati, itemsSetPrec.collect())

      if (itemSets.nonEmpty) {
        val itemSetCounts = countItemSet(itemSets, dim).filter(_._2 >= minSupport)

        if (itemSetCounts.isEmpty) {
          mapItem
        }
        else {
          aprioriIter(mapItem ++ itemSetCounts, dim + 1)
        }
      } else mapItem
    }

    def firstStep(): Map[Set[String], Int] = {
      val itemSetCount = items.map(x => (Set(x), 1))
      val itemCounts = itemSetCount.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= minSupport)
      itemCounts.collect().toMap
    }

    def avviaAlgoritmo(): Map[Set[String], Int] = {
      val itemSingoli = firstStep()

      if (itemSingoli.isEmpty) {
        itemSingoli
      }
      else {
        aprioriIter(itemSingoli, 2)
      }
    }

    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }
}
