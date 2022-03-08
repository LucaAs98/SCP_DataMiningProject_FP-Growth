package nonord

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.parallel.{ParIterable, ParMap}
import utils.Utils._
import classes.{Trie, ArrayTrie}

object NonordFPPar extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset().par

  //Elementi singoli presenti nel dataset
  val totalItem = (dataset reduce ((xs, x) => xs ++ x)).toList

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[String]): Map[String, Int] = {
    (item map (x => x -> (dataset count (y => y.contains(x))))).toMap
  }

  //Ordina gli elementi prima per numero di occorrenze, poi alfabeticamente
  def functionOrder(elem1: (String, Int), elem2: (String, Int)): Boolean = {
    if (elem1._2 == elem2._2)
      elem1._1 < elem2._1
    else
      elem1._2 > elem2._2
  }

  //Ordiniamo le transazioni in modo decrescente
  def datasetFilter(firstStep: List[String]) = {
    dataset.map(x => x.toList.filter(elemento => firstStep.contains(elemento)).
      sortBy(firstStep.indexOf(_)))
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

  //Ordiniamo i path per le occorrenze, eliminando gli item sotto il minimo supporto
  def condPBSingSort(listOfPaths: List[(List[String], Int)], elementSorted: List[String]) = {
    listOfPaths.map(elem => elem._1.filter(x => elementSorted.contains(x)).sortBy(x => elementSorted.indexOf(x)) -> elem._2)
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

  //Calcoliamo i frequentItemSet dal conditionalPB
  def createFreqItemSet(condPB: ParMap[String, List[(List[String], Int)]],
                        firstMapSorted: Map[String, (Int, Int)]): ParIterable[(List[String], Int)] = {
    condPB.map(elem => createFreqItemSetIt(elem._1, elem._2, firstMapSorted)).flatten
  }

  //Calcoliamo i frequItem per un singolo elemento del condPB
  def createFreqItemSetIt(item: String, headList: List[(List[String], Int)],
                          firstMapSorted: Map[String, (Int, Int)]): ParIterable[(List[String], Int)] = {
    //Contiamo quante volte esiste un singolo item all'interno di tutti i path nel condPB di un determinato item e rimuoviamo quelli sotto il min supp
    val itemCount = countItemConPB(headList, Map.empty[String, Int]).filter(x => x._2 >= minSupport)
    //Ordiniamo itemCount
    val itemMapSorted = ListMap(itemCount.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    //Rimuoviamo dai path che portano all'item tutti gli elementi sotto il min supp e li riordiniamo in base ad itemMapSorted
    val pathsSorted = condPBSingSort(headList, itemMapSorted.keys.toList)
    //Aggiungiamo l'indice ad itemMapSorted per facilitarci riordinamenti successivi
    val firstMapWithIndex = itemMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2))
    val trieCond = new Trie(firstMapWithIndex)
    trieCond.addPaths(pathsSorted)

    //Se l'albero appena costruito non ha più di un branch calcoliamo i freqItemSet
    if (!trieCond.moreBranch) {
      if (trieCond.nonEmptyTrie) {
        val itemsFreq = itemMapSorted.toSet
        //Vegono create tutte le possibili combinazioni tra gli item frequenti
        val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
        //Vengono ottenuti i frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
        ParIterable(subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item)._2): _*)
      } else {
        //Se l'albero è vuoto aggiungiamo ai freqItemSet l'item di cui stavamo facendo i calcoli
        ParIterable(List(item) -> firstMapSorted(item)._2)
      }
    } else {
      //Se l'albero creato ha più branch dobbiamo ricalcolare tutto
      val arrayTrieCond = new ArrayTrie(trieCond)
      val conditionalPatternBase = arrayTrieCond.createCondPBPar()
      val freqItemSet = createFreqItemSet(conditionalPatternBase, firstMapWithIndex)
      freqItemSet.map(x => (x._1 :+ item) -> x._2) ++ ParIterable(List(item) -> firstMapSorted(item)._2)
    }
  }

  //Calcoliamo startIndex partendo dal conto dei nodi di un certo item
  def calcStartIndex(startIndex: Array[Int], counterDifferentNode: mutable.Map[Int, Int]): Unit = {
    //Riempiamo le celle dello startIndex partendo da quella in posizione 1
    for (i <- 1 until startIndex.length) {
      //Prendiamo quanti nodi esistono dell'item precedente e li sommiamo all'indice di partenza di esso
      startIndex(i) = counterDifferentNode(i - 1) + startIndex(i - 1)
    }
  }



  def exec() = {
    //Calcolo della frequenza dei singoli items
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

    //Per ogni item creiamo l'indice così da facilitarci l'ordinamento più avanti
    // String -> indice, frequenza
    val firstMapWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2))

    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList).seq.toList

    val trie = new Trie(firstMapWithIndex)
    trie.addTransactions(orderDataset)

    /* Inizializzazione dell'array in cui sono contenuti gli indici, che indicano da dove iniziano le celle contigue
    * per ogni item nell'arrayTrie. */
    val arrayTrie = new ArrayTrie(trie)

    //Creiamo il conditionalPB dall'arrayTrie
    val conditionalPatternBase = arrayTrie.createCondPBPar()

    //Calcoliamo i frequentItemSet dal conditionalPB
    val frequentItemSet = createFreqItemSet(conditionalPatternBase, firstMapWithIndex)
    frequentItemSet.map(x => x._1.toSet -> x._2).toMap.seq
  }

  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "NonordFPParResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "NonordFPParSupport.txt")
}
