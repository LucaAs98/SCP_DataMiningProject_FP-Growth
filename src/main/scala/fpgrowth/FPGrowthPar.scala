package fpgrowth

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.parallel.ParMap
import utils.Utils._
import classes.Tree

object FPGrowthPar extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset().par

  //Elementi singoli presenti nel dataset
  val totalItem = (dataset reduce ((xs, x) => xs ++ x)).toList

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[String]): Map[String, Int] = {
    (item map (x => x -> (dataset count (y => y.contains(x))))).toMap
  }

  //Ordiniamo le transazioni in modo decrescente
  def datasetFilter(firstStep: List[String]) = {
    dataset.map(x => x.toList.filter(elemento => firstStep.contains(elemento)).
      sortBy(firstStep.indexOf(_)))
  }

  //Ordina gli elementi prima per numero di occorrenze, poi alfabeticamente
  def functionOrder(elem1: (String, Int), elem2: (String, Int)): Boolean = {
    if (elem1._2 == elem2._2)
      elem1._1 < elem2._1
    else
      elem1._2 > elem2._2
  }

  //Otteniamo il frequentItemSet e le sue occorenze
  def calcoloMinimi(elemento: Set[(String, Int)]) = {
    if (elemento.size > 1) {
      //Utilizzando un accumulatore otteniamo il numero di volte che gli elementi sono trovati assieme
      elemento.tail.foldLeft((List(elemento.head._1), elemento.head._2))((x, y) => (x._1 :+ y._1, x._2.min(y._2)))
    } else {
      (List(elemento.head._1), elemento.head._2)
    }
  }

  //Ordiniamo i path per le occorrenze, eliminando gli item sotto il minimo supporto
  def condPBSingSort(listOfPaths: List[(List[String], Int)], elementSorted: List[String]) = {
    listOfPaths.map(elem => elem._1.filter(elementSorted.contains).sortBy(elementSorted.indexOf(_)) -> elem._2)
  }

  //Operazione di conteggio relativa agli elementi del Conditional Pattern Base
  @tailrec
  def countItemConPB(liste: List[(List[String], Int)], acc: Map[String, Int]): Map[String, Int] = {

    //Se non è vuota
    if (liste.nonEmpty) {
      //Viene preso il primo elemento
      val head = liste.head
      //Per ogni elemento del path vengono assegnate le proprie occorrenze
      val subMap = head._1.map(x => x -> head._2).toMap
      //Vengono aggiunti gli elementi all'accumulatore e di conseguenza vengono aggiornati i valori trovati in precedenza
      val subMapFinal = acc ++ subMap.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
      //Viene richiamata la funzione prendendo tutti i path tranne il primo
      countItemConPB(liste.tail, subMapFinal)
    }
    else {
      //Se la lista è vuota viene restituito l'accumulatore
      acc
    }
  }

  //Per ogni singolo elemento viene costituito il conditional FPTree per poi calcolare gli elementi frequenti
  def freqItemsetCondPB(item: String, headList: List[(List[String], Int)], firstMapSorted: ListMap[String, Int]): List[(List[String], Int)] = {
    //Conteggio elementi relativi Conditional Pattern Base
    val itemCount = countItemConPB(headList, Map.empty[String, Int]).filter(x => x._2 >= minSupport)
    //Vengono restituiti tutti gli elementi oridinati alfabeticamente e in ordine decrescente per numero di occorrenze
    val itemMapSorted = ListMap(itemCount.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    //Otteniamo i path ordinati per le occorrenze
    val orderedPath = condPBSingSort(headList, itemMapSorted.keys.toList)

    val condTreeItem = new Tree(itemMapSorted)
    condTreeItem.addPaths(orderedPath)

    //Se l'albero creato ha un signolo branch
    if (!condTreeItem.moreBranch) {
      //Se l'ht non è vuota
      if (condTreeItem.getIfNonEmptyHt) {
        //Vengono restituiti gli elementi frequenti
        val itemsFreq = condTreeItem.getHt.map(x => x._1 -> x._2._1).toSet
        //Vegono create tutte le possibili combinazioni tra gli item frequenti
        val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
        //Vengono ottenuti i frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
        subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item))
      }
      else {
        // se ht è vuota la lista degli itemSet frequenti è composta solo dal itemSet con la sua frequenza
        List((List(item) -> firstMapSorted(item)))
      }
    } else {
      //Viene ricostruito il conditionalPB per gli item frequenti relativi al item di partenza
      val itemCresOrder = ListMap(itemMapSorted.toList.reverse: _*)
      val newCondPB = itemCresOrder.map(x => x._1 -> condTreeItem.getAllPathsFromItem(x._1)).par
      // calcolo frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
      val freqItemsetItem = condFPTree(newCondPB, itemMapSorted)
      freqItemsetItem.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item))
    }
  }


  def condFPTree(conditionalPatternBase: ParMap[String, List[(List[String], Int)]],
                 firstMapSorted: ListMap[String, Int]): List[(List[String], Int)] = {

    val freqItemsets = conditionalPatternBase.map(elem => freqItemsetCondPB(elem._1, elem._2, firstMapSorted))
    freqItemsets.flatten.toList
  }


  def exec() = {

    //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList).seq.toList

    val newTree = new Tree(firstMapSorted)

    newTree.addTransactions(orderDataset)

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

    //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
    val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> newTree.getAllPathsFromItem(x._1)).par

    //Vengono calcolati gli itemSet frequenti
    val allFreqitemset = condFPTree(conditionalPatternBase, firstMapSorted)
    //Viene restituito il frequentItemSet come una mappa
    allFreqitemset.map(x => x._1.toSet -> x._2).toMap
  }

  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthNewParResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthNewResultParSupport.txt")
}
