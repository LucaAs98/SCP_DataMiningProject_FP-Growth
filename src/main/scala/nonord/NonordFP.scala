package nonord

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import utils.Utils._
import classes.{Trie, ArrayTrie}

object NonordFP {
  def exec(minSupport: Int, pathInput:String): (Map[Set[String], Int], Long, Float) = {
    //Prendiamo il dataset (vedi Utils per dettagli)
    val (dataset, dimDataset) = prendiDataset(pathInput)

    //Elementi singoli presenti nel dataset
    val totalItem = dataset.reduce((xs, x) => xs ++ x).toList

    //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
    def countItemSet(item: List[String]): Map[String, Int] = {
      item.map(x => x -> dataset.count(y => y.contains(x))).toMap
    }

    //Ordina gli elementi prima per numero di occorrenze, poi alfabeticamente
    def functionOrder(elem1: (String, Int), elem2: (String, Int)): Boolean = {
      if (elem1._2 == elem2._2)
        elem1._1 < elem2._1
      else
        elem1._2 > elem2._2
    }

    //Ordiniamo le transazioni in modo decrescente
    def datasetFilter(firstStep: Map[String, (Int, Int)]) = {
      dataset.map(x => x.toList.filter(elemento => firstStep.contains(elemento)).
        sortBy(firstStep(_)._1))
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
    @tailrec
    def createFreqItemSet(condPB: Map[String, List[(List[String], Int)]], firstMapSorted: Map[String, (Int, Int)],
                          accFreqItemset: List[(List[String], Int)]): List[(List[String], Int)] = {
      //Scorriamo tutti gli elementi del conditionalPB
      if (condPB.nonEmpty) {
        val head = condPB.head
        //Calcoliamo i frequItem per un singolo elemento del condPB
        val freqItemSet = createFreqItemSetIt(head._1, head._2, firstMapSorted)
        //Continuiamo a calcolare i freqItemSet dell'item successsivo nel condPB
        createFreqItemSet(condPB.tail, firstMapSorted, accFreqItemset ++ freqItemSet)
      }
      else
        accFreqItemset //Restituiamo l'intera lista dei freqItemSet
    }

    //Calcoliamo i freqItem per un singolo elemento del condPB
    def createFreqItemSetIt(item: String, headList: List[(List[String], Int)], firstMapSorted: Map[String, (Int, Int)]): List[(List[String], Int)] = {
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
          subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item)._2)
        } else {
          //Se l'albero è vuoto aggiungiamo ai freqItemSet l'item di cui stavamo facendo i calcoli
          List(List(item) -> firstMapSorted(item)._2)
        }
      } else {
        //Se l'albero creato ha più branch dobbiamo ricalcolare tutto
        val arrayTrieCond = new ArrayTrie(trieCond)
        val conditionalPatternBase = arrayTrieCond.createCondPB()
        val freqItemSet = createFreqItemSet(conditionalPatternBase, firstMapWithIndex, List[(List[String], Int)]())
        freqItemSet.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item)._2)
      }
    }


    def avviaAlgoritmo(): Map[Set[String], Int] = {
      //Calcolo della frequenza dei singoli items
      val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

      //Ordina gli item dal più frequente al meno
      val firstMapSorted = firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2))

      //Per ogni item creiamo l'indice così da facilitarci l'ordinamento più avanti
      // String -> indice, frequenza
      val firstMapWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2)).toMap

      //Ordiniamo le transazioni del dataset in modo decrescente
      val orderDataset = datasetFilter(firstMapWithIndex)

      //Creiamo il trie
      val trie = new Trie(firstMapWithIndex)

      //Aggiungo le transazioni al trie
      trie.addTransactions(orderDataset)

      //Creazione dell'array che rappresenta il trie
      val arrayTrie = new ArrayTrie(trie)

      //Creazione dei conditionalPatternBase
      val conditionalPatternBase = arrayTrie.createCondPB()

      //Calcoliamo i frequent itemset
      val frequentItemSet = createFreqItemSet(conditionalPatternBase, firstMapWithIndex, List[(List[String], Int)]())
      //Risultato finale
      frequentItemSet.map(x => x._1.toSet -> x._2).toMap
    }

    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }
}
