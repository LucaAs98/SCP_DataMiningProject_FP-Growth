package nonord

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import utils.Utils._
import classes.{Trie, ArrayTrie}
import mainClass.MainClass.minSupport

object NonordFPRDD extends App {
  val sc = getSparkContext("NonordFPRDDOld")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val lines = getRDD(sc)
  val dataset = lines.map(x => x.split(" "))

  val numParts = 300

  //Contiamo, filtriamo e sortiamo tutti gli elementi nel dataset
  def getSingleItemCount(partitioner: Partitioner): Array[(String, Int)] = {
    dataset.flatMap(t => t).map(v => (v, 1))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minSupport)
      .collect()
      .sortBy(x => (-x._2, x._1))
  }

  //Ordina gli elementi prima per numero di occorrenze, poi alfabeticamente
  def functionOrder(elem1: (String, Int), elem2: (String, Int)): Boolean = {
    if (elem1._2 == elem2._2)
      elem1._1 < elem2._1
    else
      elem1._2 > elem2._2
  }


  //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
  def genCondTransactions(transaction: Array[String],
                          itemToRank: ListMap[String, (Int, Int)],
                          partitioner: Partitioner): mutable.Map[Int, List[String]] = {
    //Mappa finale da riempire
    val output = mutable.Map.empty[Int, List[String]]
    // Ordiniamo le transazioni in base alla frequenza degli item ed eliminando gli item non frequenti
    val transFiltered = transaction.filter(item => itemToRank.contains(item)).sortBy(item => itemToRank(item)._1)
    val n = transFiltered.length
    var i = n - 1
    while (i >= 0) {
      val item = transFiltered(i)
      //Prendiamo la partizione alla quale fa parte l'item
      val part = partitioner.getPartition(item)
      /* Se la mappa non contiene già un elemento con l'indice della partizione come chiave, andiamo a mettere quella parte
      * di transazione nella mappa. */
      if (!output.contains(part)) {
        output(part) = transFiltered.slice(0, i + 1).toList
      }
      i -= 1
    }
    output
  }

  //Restituisce tutti gli item singoli all'interno delle liste di path
  def totalItem(arrayPaths: Array[(List[String], Int)]): List[String] =
    arrayPaths.foldLeft(Set[String]())((xs, x) => xs ++ x._1).toList

  //Occorrenze totali di quell'item
  def countItemNodeFreq(array: Array[(String, Int, Int)]): Int = {
    array.foldLeft(0)((x, y) => x + y._2)
  }

  //Operazione di conteggio relativa agli elementi del Conditional Pattern Base
  @tailrec
  def countItemConPB(arrayPercorsi: Array[(List[String], Int)], acc: Map[String, Int]): Map[String, Int] = {
    //Se non è vuota
    if (arrayPercorsi.nonEmpty) {
      //Viene preso il primo elemento
      val head = arrayPercorsi.head
      //Per ogni elemento del path vengono assegnate le proprie occorrenze
      val subMap = head._1.map(x => x -> head._2).toMap
      //Vengono aggiunti gli elementi all'accumulatore e di conseguenza vengono aggiornati i valori trovati in precedenza
      val subMapFinal = acc ++ subMap.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
      //Viene richiamata la funzione prendendo tutti i path tranne il primo
      countItemConPB(arrayPercorsi.tail, subMapFinal)
    }
    else {
      //Se la lista è vuota viene restituito l'accumulatore
      acc
    }
  }

  //Ordiniamo i path per le occorrenze, eliminando gli item sotto il minimo supporto
  def condPBSingSort(arrayOfPaths: Array[(List[String], Int)], elementSorted: List[String]) = {
    arrayOfPaths.map(elem => elem._1.filter(x => elementSorted.contains(x)).sortBy(x => elementSorted.indexOf(x)) -> elem._2)
  }


  //Calcoliamo i frequentItemSet
  def createFreqItemSet(arrayTrie: ArrayTrie, countDiffNode: mutable.Map[Int, Int],
                        validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {

    val indexToItem = arrayTrie.getIndexToItem

    countDiffNode.iterator.flatMap {
      case (indexItem, itemDiffNode) =>
        val stringItem = indexToItem(indexItem)
        val nodesItem = arrayTrie.getNodesItem(indexItem, itemDiffNode)

        val frequenzaItem = countItemNodeFreq(nodesItem)

        /* Controlliamo che quel determinato item sia della partizione che stiamo considerando e quelli sotto il min supp
        * (Dopo il primo passaggio validateSuffix, sarà sempre true). */
        if (validateSuffix(stringItem) && frequenzaItem >= minSupport) {
          //Prendiamo tutti i percorsi per quel determinato item senza quest'ultimo
          val lPerc = nodesItem.map(nodo => arrayTrie.getPercorso(nodo._3, List[String]()) -> nodo._2)

          //Contiamo quante volte esiste un singolo item all'interno di tutti i path nel condPB di un determinato item e rimuoviamo quelli sotto il min supp
          val itemCount = countItemConPB(lPerc, Map.empty[String, Int])

          //Aggiungiamo l'indice ad itemMapSorted per facilitarci riordinamenti successivi
          val firstMapWithIndexItem = itemCount.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2))

          //Creiamo i conditional trie
          val condTrie = new Trie(firstMapWithIndexItem)
          //Aggiungiamo i path ai condTrie
          condTrie.addPaths(lPerc.toList)

          //Creazione arrayTrie condizionali
          val arrayTrieCond = new ArrayTrie(condTrie)

          //Creazione dei freqItemSet
          Iterator.single((stringItem :: Nil, frequenzaItem)) ++ createFreqItemSet(arrayTrieCond, condTrie.counterDifferentNode).map {
            case (t, c) => (stringItem :: t, c)
          }
        } else {
          //Se non fa parte di quel gruppo restituiamo iterator vuoto
          Iterator.empty
        }
    }
  }


  def exec() = {
    //Creiamo il partitioner
    val partitioner = new HashPartitioner(numParts)

    //Prendiamo tutti gli elementi singoli con il loro count
    val firstStepVar = getSingleItemCount(partitioner)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStepVar: _*)

    //Aggiungiamo l'indice agli item per facilitarci l'ordinamento
    val itemToRank = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2))

    //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
    val condTrans = dataset.flatMap(transaction => genCondTransactions(transaction, itemToRank, partitioner))

    //Transazioni condizionali raggruppate per partizione
    val condTransGrouped = condTrans.groupByKey(partitioner.numPartitions)

    //Creazione arrayTrie per ogni gruppo
    val condArrayStartTrie = condTransGrouped.map(group =>
      group._1 -> {
        //Per ogni gruppo creiamo il trie
        val trieGroup = new Trie(itemToRank)
        //Gli aggiungiamo le transazioni
        trieGroup.addTransactions(group._2.toList)
        //Ripuliamo item map e riordiniamo gli indici degli item riordinati
        trieGroup.clearitToMapAndCDifNod()
        //Creiamo l'arrayTrie per ogni gruppo
        val arrayTrieGroup = new ArrayTrie(trieGroup)

        //Restituiamo l'arrayTrie ed il numero di nodi del trie del gruppo
        (arrayTrieGroup, trieGroup.counterDifferentNode)
      })


    //Caloliamo i frequentItemSet
    val freqItemSet = condArrayStartTrie.flatMap(elem => createFreqItemSet(elem._2._1, elem._2._2, x => partitioner.getPartition(x) == elem._1))

    freqItemSet.map(x => x._1.toSet -> x._2).collect().toMap
  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "NonordFPRDDResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "NonordFPRDDSupport.txt")
}