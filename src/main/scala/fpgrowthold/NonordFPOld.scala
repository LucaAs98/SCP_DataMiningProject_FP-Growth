package fpgrowthold

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import utils.Utils._
import classes.Node
import mainClass.MainClass.minSupport

object NonordFPOld extends App {

  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset()

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

  /* Creazione di tutto l'albero. */
  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: List[Set[String]], itemMap: Map[String, (Int, Int)], count: Int, counterDifferentNode: mutable.Map[Int, Int]): Int = {
    if (transactions.nonEmpty) {
      //Singola transazione togliendo gli elementi che non sonon sopra il min supp e riordinandola per frequenza
      val head = transactions.head.filter(itemMap.contains).toList.sortBy(itemMap(_)._1)
      //Aggiungiamo i nodi della transazione all'albero, contiamo quanti nodi nuovi abbiamo aggiunto
      val newCount = addNodeTransaction(tree, head, count, itemMap, counterDifferentNode)
      //Una volta aggiunta una transazione continuiamo con le successive
      creazioneAlbero(tree, transactions.tail, itemMap, newCount, counterDifferentNode)
    }
    else
      count
  }

  /* Aggiungiamo tutti i nodi di una transazione. */
  @tailrec
  def addNodeTransaction(lastNode: Node[String], transazione: List[String], count: Int, itemMap: Map[String, (Int, Int)], counterDifferentNode: mutable.Map[Int, Int]): Int = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val (node, flagNewNode) = lastNode.add(transazione.head)

      //Aggiungiamo al conto dei nodi totali il numero dei nuovi nodi aggiunti di volta in volta
      val newCount = count + {
        if (flagNewNode) {
          counterDifferentNode(itemMap(node.value)._1) += 1
          1
        } else 0
      }

      //Continuiamo ad aggiungere i nodi successivi della transazione
      addNodeTransaction(node, transazione.tail, newCount, itemMap, counterDifferentNode)
    }
    else
      count
  }

  //Creiamo il conditionalPB
  def createCondPB(arrayTrie: Array[(String, Int, Int)]): Map[String, List[(List[String], Int)]] = {
    val mapItemPath = arrayTrie.map(elem => elem._1 ->
      (itCreateConditionalPatternBase(elem._3, arrayTrie, List.empty[String]), elem._2)).groupBy(_._1)

    val mapItemPathMapped = mapItemPath.map(elem => elem._1 -> elem._2.map(_._2).toList)

    mapItemPathMapped
  }

  //Creazione di un percorso dato un elemento, simile a listaPercorsi, ma per l'arrayTrie
  @tailrec
  def itCreateConditionalPatternBase(parent: Int, arrayTrie: Array[(String, Int, Int)], acc: List[String]): List[String] = {
    if (parent != -1) {
      val last = arrayTrie(parent)
      itCreateConditionalPatternBase(last._3, arrayTrie, (last._1) :: acc)
    } else {
      List.empty[String] ::: acc
    }
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

  //Aggiungiamo il singolo nodo per il path (Siamo nella creazione dell'albero per il singolo item)
  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int, flag: Boolean, count: Int,
                  counterDifferentNode: mutable.Map[Int, Int], itemMap: ListMap[String, (Int, Int)]): (Boolean, Int) = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val (node, flagNewNode) = lastNode.add(path.head, countPath)
      //Viene controllato se sono presenti altri branch
      val moreBranch = {
        if (!flag) lastNode.sons.size > 1
        else flag
      }

      //Contiamo i nodi distinti
      val newCount = count + {
        if (flagNewNode) {
          counterDifferentNode(itemMap(node.value)._1) += 1
          1
        } else 0
      }

      //Continuiamo a scorrere il path
      addNodePath(node, path.tail, countPath, moreBranch, newCount, counterDifferentNode, itemMap)
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituito il flag relativo alla formazione di nuovi branch ed il count dei nodi distinti
      (flag, count)
    }
  }

  //Creazione dell'albero per il singolo item, simile alla creazione dell'albero iniziale
  @tailrec
  def creazioneAlberoItem(tree: Node[String], sortedPaths: List[(List[String], Int)], flag: Boolean, count: Int,
                          counterDifferentNode: mutable.Map[Int, Int], itemMap: ListMap[String, (Int, Int)]): (Boolean, Int) = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      val (moreBranch, newCount) = addNodePath(tree, head._1, head._2, flag, count, counterDifferentNode, itemMap)
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, sortedPaths.tail, moreBranch, newCount, counterDifferentNode, itemMap)
    } else (flag, count) //Esaminati tutti i path restituiamo il flag dei branch ed il numero di nodi distinti
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
    //Contiamo quanti nodi distinti abbiamo per ogni item, ci servirà nel caso in cui dobbiamo continuare (più branch)
    val counterDifferentNode = mutable.Map(firstMapWithIndex.map(_._2._1 -> 0).toSeq: _*)
    //Creiamo il nuovo albero
    val tree = new Node[String](null, List())
    val (moreBranch, nNodi) = creazioneAlberoItem(tree, pathsSorted, flag = false, 0, counterDifferentNode, firstMapWithIndex)

    //Se l'albero appena costruito non ha più di un branch calcoliamo i freqItemSet
    if (!moreBranch) {
      if (tree.sons.nonEmpty) {
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
      val startIndex = new Array[Int](itemMapSorted.size)
      val arrayTrie = new Array[(String, Int, Int)](nNodi)
      calcStartIndex(startIndex, counterDifferentNode)
      createTrie(arrayTrie, tree, startIndex, firstMapWithIndex, -1)
      val conditionalPatternBase = createCondPB(arrayTrie)
      val freqItemSet = createFreqItemSet(conditionalPatternBase, firstMapWithIndex, List[(List[String], Int)]())
      freqItemSet.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item)._2)
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

  //Mettiamo l'elemento nell'arrayTrie
  @tailrec
  def putElementInArray(arrayTrie: Array[(String, Int, Int)], index: Int, node: Node[String], parent: Int): Int = {
    //Se la posizione in cui doobiamo iniziare a mettere gli item con tale valore non è occupata, lo inseriamo
    if (arrayTrie(index) == null) {
      arrayTrie(index) = (node.value, node.occurrence, parent)
      //Rerstituiamo l'indice in modo tale da ricavarci quale sarà il padre del nodo successivo (dato che è suo figlio)
      index
    } else {
      //Se la cella è già occupata, andiamo alla successiva
      putElementInArray(arrayTrie, index + 1, node, parent)
    }
  }


  //Aggiorniamo l'arrayTrie per ogni figlio
  @tailrec
  def createTrieSons(arrayTrie: Array[(String, Int, Int)], sons: List[Node[String]], startIndex: Array[Int],
                     firstMapWithIndex: Map[String, (Int, Int)], parentIndex: Int): Unit = {
    //Scorriamo tutti i figli
    if (sons.nonEmpty) {
      val firstSon = sons.head //Prendiamo il primo figlio
      //Aggiorniamo l'array anche per ogni figlio dei figli
      createTrie(arrayTrie, firstSon, startIndex, firstMapWithIndex, parentIndex)
      //Aggiorniamo l'array per ogni figlio
      createTrieSons(arrayTrie, sons.tail, startIndex, firstMapWithIndex, parentIndex)
    }
  }

  //Riempiamo l'arrayTrie
  def createTrie(arrayTrie: Array[(String, Int, Int)], lastNode: Node[String], startIndex: Array[Int],
                 firstMapWithIndex: Map[String, (Int, Int)], parentIndex: Int): Unit = {
    //Se non è la radice dell'albero
    if (!lastNode.isHead) {
      //Prendiamo l'indice "di ordinamento" dell'item
      val index = firstMapWithIndex(lastNode.value)._1
      //Prendiamo l'indice di partenza dell'item
      val firstIndex = startIndex(index)
      //Mettiamo l'elemento nell'arrayTrie e restituiamo il padre del nodo successivo che andremo ad inserire
      val newParent = putElementInArray(arrayTrie, firstIndex, lastNode, parentIndex)
      //Aggiungiamo all'array anche i figli
      createTrieSons(arrayTrie, lastNode.sons, startIndex, firstMapWithIndex, newParent)
    } else {
      //Se è la radice dobbiamo aggiornare l'arrayTrie per i figli della radice
      createTrieSons(arrayTrie, lastNode.sons, startIndex, firstMapWithIndex, parentIndex)
    }
  }

  //Metodo per la stampa dell'albero
  def printTreeRec(node: Node[String], str: String): Unit = {
    if (node.occurrence != -1) {
      println(str + node.value + " " + node.occurrence)
      node.sons.foreach(printTreeRec(_, str + "\t"))
    }
    else {
      node.sons.foreach(printTreeRec(_, str))
    }
  }


  def exec() = {

    //Calcolo della frequenza dei singoli items
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2))

    //Per ogni item creiamo l'indice così da facilitarci l'ordinamento più avanti
    // String -> indice, frequenza
    val firstMapWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2)).toMap

    //Creiamo il nostro albero vuoto
    val tree = new Node[String](null, List())

    //Mappa che contiene tutti i nodi dell'albero distinti (anche i nodi con stesso item, ma in diversa posizione)
    val counterDifferentNode = mutable.Map(firstMapWithIndex.map(_._2._1 -> 0).toSeq: _*)

    //Scorriamo tutte le transazioni creando il nostro albero. Restituiamo il numero di nodi distinti
    val numeroNodi = creazioneAlbero(tree, dataset, firstMapWithIndex, 0, counterDifferentNode)

    /* Inizializzazione dell'array in cui sono contenuti gli indici, che indicano da dove iniziano le celle contigue
    * per ogni item nell'arrayTrie. */
    val startIndex = new Array[Int](firstMapWithIndex.size)

    //Calcoliamo startIndex partendo dal conto dei nodi di un certo item
    calcStartIndex(startIndex, counterDifferentNode)

    //Inizializziamo l'arrayTrie con il numero di nodi calcolati in precedenza
    val arrayTrie = new Array[(String, Int, Int)](numeroNodi)

    //Riempiamo l'arrayTrie
    createTrie(arrayTrie, tree, startIndex, firstMapWithIndex, -1)

    //Creiamo il conditionalPB dall'arrayTrie
    val conditionalPatternBase = createCondPB(arrayTrie)

    //Calcoliamo i frequentItemSet dal conditionalPB

    val frequentItemSet = createFreqItemSet(conditionalPatternBase, firstMapWithIndex, List[(List[String], Int)]())
    frequentItemSet.map(x => x._1.toSet -> x._2).toMap
  }

  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "NonordFPResultOld.txt")
  scriviSuFileSupporto(result, numTransazioni, "NonordFPSupportOld.txt")
}
