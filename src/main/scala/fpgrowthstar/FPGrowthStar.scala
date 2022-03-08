package fpgrowthstar

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import utils.Utils._
import classes.{TreeStar,Node}

object FPGrowthStar extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset()

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

  //Risaliamo l'albero per restituire il percorso inerente ad un nodo specifico
  @tailrec
  def listaPercorsi(nodo: Node[String], listaPercorsoAcc: List[String]): List[String] = {
    if (!nodo.padre.isHead) //Se non è il primo nodo
      listaPercorsi(nodo.padre, nodo.padre.value :: listaPercorsoAcc) //Continuiamo a risalire l'albero col padre
    else
      listaPercorsoAcc //Restituiamo tutto il percorso trovato
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

  //Per ogni nodo della lista vengono calcolati i path relativi e ordinati
  @tailrec
  def getListOfPaths(linkedList: List[Node[String]], itemsStringFreqSorted: ListMap[String, (Int, Int)], accPaths: List[(List[String], Int)]): List[(List[String], Int)] = {
    if (linkedList.nonEmpty) {
      val head = linkedList.head
      //Calcolo del path
      val path = listaPercorsi(head, List[String]())
      //Ordinamento del path
      val pathOrdered = (path.filter(x => itemsStringFreqSorted.contains(x)).sortBy(elem => itemsStringFreqSorted(elem)._2), head.occurrence)
      getListOfPaths(linkedList.tail, itemsStringFreqSorted, accPaths :+ pathOrdered)
    }
    else {
      //restituzione della lista dei path
      accPaths
    }
  }

  //Calcolo dei frquentItemSet
  @tailrec
  def calcFreqItemset(listItem: List[String], tree: TreeStar, accFreqItemset: List[(List[String], Int)]): List[(List[String], Int)] = {

    if (listItem.nonEmpty) {
      val headerTable = tree.getHt
      //Primo item
      val head = listItem.head
      //Creazione mappa da indice -> item
      val indexToItemMap = headerTable.map(x => x._2._2 -> x._1)
      //lista dei nodi dove è presente l'item
      val linkedList = headerTable(head)._3
      //Elementi che frequentemente  sono accoppiati con l'item esaminato
      val itemFreqItem = tree.getFreqItems(head)

      //Calcolo dei ItemSet frequenti per l'item analizzato
      val freqItemSetSingle = fpGrowthStarCore(head, headerTable(head)._1, itemFreqItem, indexToItemMap, linkedList)
      calcFreqItemset(listItem.tail, tree, accFreqItemset ++ freqItemSetSingle)
    } else {
      accFreqItemset
    }
  }


  def fpGrowthStarCore(item: String, freq: Int, itemFreqItem: List[Int], indexToItemMap: ListMap[Int, String], linkedList: List[Node[String]]) = {

    if (itemFreqItem.nonEmpty) {
      //Ricaviamo l'item sottoforma di stringa dagli indici degli array, ordinati per occorrenze
      val itemsStringFreq = itemFreqItem.zipWithIndex.map(x => indexToItemMap(x._2) -> x._1)
      val itemsStringFreqSorted = ListMap(itemsStringFreq.filter(_._2 >= minSupport).sortWith((elem1, elem2) => functionOrder(elem1, elem2)).zipWithIndex
        .map(x => x._1._1 -> (x._1._2, x._2)): _*)
      //Creazione della matrice, ht e dell'albero

      val treeItem = new TreeStar(itemsStringFreqSorted)

      //Viene restituita la lista dei path relativa all'item
      val listOfPaths = getListOfPaths(linkedList, itemsStringFreqSorted, List[(List[String], Int)]())
      //Inserimento dei path nell'albero
      treeItem.addPaths(listOfPaths)

      //Se l'albero ha solo un branch
      if (!treeItem.moreBranch) {
        //Se l'ht non è vuota
        if (treeItem.getIfNonEmptyHt) {
          //Vengono restituiti gli elementi frequenti
          val itemsFreq = treeItem.getHt.map(x => x._1 -> x._2._1).toSet
          //Vegono create tutte le possibili combinazioni tra gli item frequenti
          val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
          //Vengono ottenuti i frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
          subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> freq)
        }
        else {
          // Se ht è vuota la lista degli itemSet frequenti è composta solo dal itemSet con la sua frequenza
          List((List(item) -> freq))
        }
      } else {
        // Calcolo frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
        val itemsetFreq = calcFreqItemset(treeItem.getHt.keys.toList, treeItem, List[(List[String], Int)]())
        itemsetFreq.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> freq)
      }
    }
    else {
      // Se la matrice è vuota la lista degli itemSet frequenti è composta solo dal itemSet con la sua frequenza
      List(List(item) -> freq)
    }


  }

  def exec() = {

    //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList)

    // Vengono restituiti gli elementi con la loro frequenza e l'indice relativo ad essi
    //Item -> (Frequenza, Indice)
    val firstMapSortedWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2))

    val newTree = new TreeStar(firstMapSortedWithIndex)

    newTree.addTransactions(orderDataset)

    //Vengono calcolati gli itemSet frequenti
    val allFreqitemset = calcFreqItemset(newTree.getHt.keys.toList, newTree, List[(List[String], Int)]())
    //Viene restituito il frequentItemSet come una mappa
    allFreqitemset.map(x => x._1.toSet -> x._2).toMap

  }

  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthStarResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultStarSupport.txt")
}
