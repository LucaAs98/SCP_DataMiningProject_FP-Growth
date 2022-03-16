package fpgrowthmod

import classes.Tree
import utils.Utils._

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import mainClass.MainClass.minSupport

object FPGrowthMod {
  //Esecuzione effettiva dell'algoritmo
  def exec(): (Map[Set[String], Int], Long, Float) = {

    //Prendiamo il dataset (vedi Utils per dettagli)
    val (dataset, dimDataset) = prendiDataset()

    //Elementi singoli presenti nel dataset
    val totalItem = dataset.reduce((xs, x) => xs ++ x).toList

    //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
    def countItemSet(item: List[String]): Map[String, Int] = {
      item.map(x => x -> dataset.count(y => y.contains(x))).toMap
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

    /* Calcolo Frequent ItemSet per un singolo item. */
    @tailrec
    def freqItemSetOneItem(item: String, oneCondPatt: List[(List[String], Int)], accFreqItemSet: Map[Set[String], Int]): Map[Set[String], Int] = {
      if (oneCondPatt.nonEmpty) {
        //Prendiamo un percorso
        val head = oneCondPatt.head
        //Creiamo i subset con gli elementi di tale percorso + l'item preso in considerazione
        val subMap = head._1.toSet.subsets().map(elem => elem + item -> head._2).filter(_._1.nonEmpty).toMap
        //Aggiorno l'accumulatore dei frequent itemset trovati incrementando il valore delle occorrenze per tale subset
        val subMapFinal = accFreqItemSet ++ subMap.map { case (k, v) => k -> (v + accFreqItemSet.getOrElse(k, 0)) }
        //Richiamiamo questo metodo ricorsivamente su i restanti path
        freqItemSetOneItem(item, oneCondPatt.tail, subMapFinal)
      } else {
        //Restituiamo i freq itemset per un determinato item
        accFreqItemSet
      }
    }

    /* Calcolo Frequent Itemset. */
    @tailrec
    def calcFreqItemSet(cpb: ListMap[String, List[(List[String], Int)]], accFreqItemset: Map[Set[String], Int]): Map[Set[String], Int] = {
      if (cpb.nonEmpty) {
        //Prendiamo un cpb
        val oneCpb = cpb.head
        //Calcoliamo tutti i freq itemset per tale cpb
        val freqItemset = freqItemSetOneItem(oneCpb._1, oneCpb._2, Map[Set[String], Int]()).filter(item => item._2 >= minSupport)
        //Aggiungiamo i freq itemset di tale item a tutti quelli trovati in precedenza
        val newAcc = accFreqItemset ++ freqItemset
        //Continuiamo con i restanti item
        calcFreqItemSet(cpb.tail, newAcc)
      }
      else {
        //Restituiamo i freq itemset finali
        accFreqItemset
      }
    }


    def avviaAlgoritmo(): Map[Set[String], Int] = {
      //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
      val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

      //Ordina gli item dal più frequente al meno
      val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

      //Ordiniamo le transazioni del dataset in modo decrescente
      val orderDataset = datasetFilter(firstMapSorted.keys.toList)

      //Creiamo il nostro albero vuoto
      val newTree = new Tree(firstMapSorted)

      //Aggiungiamo le transazioni al nuovo albero
      newTree.addTransactions(orderDataset)

      //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
      val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

      //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
      val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> newTree.getAllPathsFromItem(x._1))

      //Calcoliamo il nostro risultato finale
      val frequentItemSet = calcFreqItemSet(conditionalPatternBase, Map[Set[String], Int]())

      frequentItemSet
    }

    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }
}
