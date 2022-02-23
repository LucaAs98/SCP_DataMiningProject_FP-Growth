import Utils._

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable

object Nonordfp extends App {

  val dataset = Utils.prendiDataset("T10I4D100K.txt")

  val dataset2 =
    List(Set("a", "c", "d", "f", "g", "i", "m", "p")
      , Set("a", "b", "c", "f", "i", "m", "o")
      , Set("b", "f", "h", "j", "o")
      , Set("b", "c", "k", "s", "p")
      , Set("a", "c", "e", "f", "l", "m", "n", "p"))

  val totalItem = (dataset reduce ((xs, x) => xs ++ x)).toList

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

  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: List[Set[String]], itemMap: Map[String, (Int, Int)], count: Int, counterDifferentNode: mutable.Map[Int, Int]): Int = {
    if (transactions.nonEmpty) {
      val head = transactions.head.filter(itemMap.contains).toList.sortBy(itemMap(_)._1) //Singola transazione
      //println(head)
      val newCount = addNodeTransaction(tree, head, count, itemMap, counterDifferentNode) //Ricorsivo su tutta la transazione
      creazioneAlbero(tree, transactions.tail, itemMap, newCount, counterDifferentNode) //Una volta aggiunta una transazione continuiamo con le successive
    }
    else
      count // else headerTable //Finite tutte le transazioni del dataset restituiamo l'ht
  }

  @tailrec
  def addNodeTransaction(lastNode: Node[String], transazione: List[String], count: Int, itemMap: Map[String, (Int, Int)], counterDifferentNode: mutable.Map[Int, Int]): Int = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)

      val newCount = count + {
        if (node._2) {
          counterDifferentNode(itemMap(node._1.value)._1) += 1
          1
        } else 0
      }

      //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
      addNodeTransaction(node._1, transazione.tail, newCount, itemMap, counterDifferentNode)
    }
    else {
      count
    }
  }

  def printTree(tree: Node[String], str: String): Unit = {
    if (tree.occurrence != -1) {
      println(str + tree.value + " " + tree.occurrence)
      tree.sons.foreach(printTree(_, str + "\t"))
    }
    else {
      tree.sons.foreach(printTree(_, str))
    }
  }

  def searchLastIndex(startIndex: Array[Int], theIndex: Int): Int = {
    //Ritorna il primo indice di startIndex se questo ha un valore diverso da -1
    // (Significa che almeno un elemento di quell'elemento è stato inserito)
    val app = startIndex.drop(theIndex + 1).indexWhere(_ != -1)
    if (app != -1)
      theIndex + app + 1 //indice restaurato
    else
      app
  }

  @tailrec
  def createTrieSons(sons: List[Node[String]], dadIndex: Int, startIndex: Array[Int], listNodes: List[(String, Int, Int)],
                     firstMapWithIndex: Map[String, (Int, Int)]): List[(String, Int, Int)] = {
    if (sons.nonEmpty) { //Se la lista dei figli non è vuota
      //Inserimento nella struttura del primo figlio nella lista
      val newList = createTrie(sons.head, dadIndex, startIndex, listNodes, firstMapWithIndex)

      //si richiama la funzione sul resto dei figli
      createTrieSons(sons.tail, dadIndex, startIndex, newList, firstMapWithIndex)
    } else {
      listNodes //Se la lista dei figli è vuota, si restituisce la lista dei nodi creata
    }
  }

  //Insieirmento nel trie(che è un array) del singono nodo
  def createTrie(lastNode: Node[String], dadIndex: Int, startIndex: Array[Int], listNodes: List[(String, Int, Int)],
                 firstMapWithIndex: Map[String, (Int, Int)]): List[(String, Int, Int)] = {
    //verifica se il nodo è testa
    if (lastNode.isHead) { //se sei testa passa subito ai figli
      createTrieSons(lastNode.sons, dadIndex, startIndex, listNodes, firstMapWithIndex)
    }
    else {
      if (listNodes.isEmpty) { //Se la lista contentente i nodi è ancora vuota, inseriamo il primo elemento
        val newList = List[(String, Int, Int)]((lastNode.value, lastNode.occurrence, -1)) //creazione lista singolo elemento
        startIndex(firstMapWithIndex(lastNode.value)._1) = 0 //Aggiornamento dell'array, il primo elemento sarà all'indice zero
        createTrieSons(lastNode.sons, 0, startIndex, newList, firstMapWithIndex) //Si passa ai figli del nodo
      } else { //Se la lista dei nodi finale non è vuota

        //Indice dell'elemento
        val valueIndex = firstMapWithIndex(lastNode.value)._1

        //Si ricerca l'indice del primo elemento prima del quale andremo ad inserire il nuovo nodo
        val newIndex = searchLastIndex(startIndex, valueIndex)

        //Se non c'è alcun elemento dopo avviene l'inserimento in coda della lista
        if (newIndex == -1) {
          val newList = listNodes :+ (lastNode.value, lastNode.occurrence, dadIndex)
          if (startIndex(valueIndex) == -1) startIndex(valueIndex) = newList.size - 1 //aggiornamento dello startIndex se non era presente alcun nodo di quell'elemento
          createTrieSons(lastNode.sons, newList.size - 1, startIndex, newList, firstMapWithIndex) //Si passa ai figli del nodo
        }
        else {
          val indexInTheList = startIndex(newIndex) //Si prende l'indice da dove iniziano gli elementi da shiftare
          val lastNodesList = updateParents(listNodes.drop(indexInTheList), indexInTheList) // si fa l'update dei genitori dei nodi shiftati
          //Inserimento in mezzo dei nodi
          val newList = (listNodes.take(indexInTheList) :+ (lastNode.value, lastNode.occurrence, dadIndex)) ::: lastNodesList
          if (startIndex(valueIndex) == -1) startIndex(valueIndex) = indexInTheList //Start index aggiornato
          updateIndex(startIndex, newIndex) //Si aggiornano tutti gli indici successivi a causa dello shift
          createTrieSons(lastNode.sons, indexInTheList, startIndex, newList, firstMapWithIndex) // si passa ai figli
        }
      }
    }
  }

  def updateIndex(startIndex: Array[Int], indexStart: Int): Unit = {
    for (i <- indexStart until startIndex.length) {
      if (startIndex(i) != -1)
        startIndex(i) += 1
    }
  }

  def updateParents(listShift: List[(String, Int, Int)], firstIndex: Int): List[(String, Int, Int)] = {
    listShift.map(x => (x._1, x._2, {
      if (x._3 >= firstIndex) x._3 + 1 else x._3
    }))
  }

  //Creazione del conditionalPattern base scorrendo il trie(la lista)
  //TrieIndex parte dalla fine fino a zero
  @tailrec
  def createConditionalPatternBase(trie: Array[(String, Int, Int)], trieIndex: Int, acc: ListMap[String, List[(List[String], Int)]]):
  ListMap[String, List[(List[String], Int)]] = {
    if (trieIndex >= 0) { //Se non siamo passati su tutti i nodi

      val last = trie(trieIndex) //Si prende l'elemento dal trie
      if (last._3 != -1) { //Si controlla il padre, ovvero se è il nodo è nullo oppure no
        //Creazione del percorso che porta a quel'elemento
        val list = (itCreateConditionalPatternBase(trie(last._3), trie, List.empty[String]), last._2)
        //Aggiornamento dell'accumulatore
        val newMap = acc.updated(last._1, acc.getOrElse(last._1, List.empty[(List[String], Int)]) :+ (list))
        createConditionalPatternBase(trie, trieIndex - 1, newMap) //si passa all'elemento successivo
      } else {
        //Creazione della lista vuota se non si ha un genitore
        val newMap = acc.updated(last._1, acc.getOrElse(last._1, acc.getOrElse(last._1, List.empty[(List[String], Int)]))
          :+ (List.empty[String], last._2))
        createConditionalPatternBase(trie, trieIndex - 1, newMap) // si passa al prossimo elemento
      }
    }
    else //Si restituisce l'accumulatore come finisce la lista dei nodi
      acc
  }

  //Creazione di un percorso dato un elemento
  @tailrec
  def itCreateConditionalPatternBase(last: (String, Int, Int), trie: Array[(String, Int, Int)], acc: List[String]): List[String] = {
    if (last._3 != -1) { // si risale il padre, creando dunque come accumulatore una lista di stringhe, finchè non si trova un padre figlio
      // del nodo null
      itCreateConditionalPatternBase(trie(last._3), trie, (last._1) :: acc)
    }
    else
      (last._1) :: acc
  }

  //dati una serie di percorsi che portano ad un elemento, si creano gli itemset frequenti
  @tailrec
  def itemSetFromOne(item: String, oneCondPatt: List[(List[String], Int)], accSubMap: Map[Set[String], Int]): Map[Set[String], Int] = {
    if (oneCondPatt.nonEmpty) {
      val head = oneCondPatt.head
      val subMap = head._1.toSet.subsets().map(elem => elem + item -> head._2).filter(_._1.nonEmpty).toMap
      val subMapFinal = accSubMap ++ subMap.map { case (k, v) => k -> (v + accSubMap.getOrElse(k, 0)) }
      itemSetFromOne(item, oneCondPatt.tail, subMapFinal)
    } else {
      accSubMap
    }
  }


  @tailrec
  def itemSetFromOneRec(cpb: ListMap[String, List[(List[String], Int)]], acc: Map[Set[String], Int]): Map[Set[String], Int] = {
    if (cpb.nonEmpty) {
      val elem = cpb.head
      //println(elem._1)
      val freqItemset = itemSetFromOne(elem._1, elem._2, Map[Set[String], Int]()).filter(item => item._2 >= minSupport)
      val newMap = acc ++ freqItemset
      itemSetFromOneRec(cpb.tail, newMap)
    }
    else {
      acc
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

  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int, flag: Boolean, count: Int,
                  counterDifferentNode: mutable.Map[Int, Int], itemMap: ListMap[String, (Int, Int)]): (Boolean, Int) = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val node = lastNode.add(path.head, countPath)
      //Viene controllato se sono presenti altri branch
      val newFlag = {
        if (!flag) lastNode.sons.size > 1
        else flag
      }

      val newCount = count + {
        if (node._2) {
          counterDifferentNode(itemMap(node._1.value)._1) += 1
          1
        } else 0
      }

      //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
      addNodePath(node._1, path.tail, countPath, newFlag, newCount, counterDifferentNode, itemMap)
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituita l' ht e il flag relativo alla formazione di nuovi branch
      (flag, count)
    }
  }

  @tailrec
  def creazioneAlberoItem(tree: Node[String], sortedPaths: List[(List[String], Int)], flag: Boolean, count: Int,
                          counterDifferentNode: mutable.Map[Int, Int], itemMap: ListMap[String, (Int, Int)]): (Boolean, Int) = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      val (newFlag, newCount) = addNodePath(tree, head._1, head._2, flag, count, counterDifferentNode, itemMap)
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, sortedPaths.tail, newFlag, newCount, counterDifferentNode, itemMap)
    } else (flag, count) //Esaminati tutti i path restituiamo ht e il flag dei branch
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

  @tailrec
  def createFreqItemSet(condDB: ListMap[String, List[(List[String], Int)]], firstMapSorted: Map[String, (Int, Int)],
                        accFreqItemset: List[(List[String], Int)]): List[(List[String], Int)] = {
    if (condDB.nonEmpty) {

      val head = condDB.head
      println(head._1)
      val freqItemset = createFreqItemSetIt(head._1, head._2, firstMapSorted)
      createFreqItemSet(condDB.tail, firstMapSorted, accFreqItemset ++ freqItemset)
    }
    else
      accFreqItemset
  }


  def createFreqItemSetIt(item: String, headList: List[(List[String], Int)], firstMapSorted: Map[String, (Int, Int)]): List[(List[String], Int)] = {

    val itemCount = countItemConPB(headList, Map.empty[String, Int]).filter(x => x._2 >= minSupport)
    val itemMapSorted = ListMap(itemCount.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    val pathsSorted = condPBSingSort(headList, itemMapSorted.keys.toList)
    val firstMapWithIndex = itemMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2))
    val counterDifferentNode = mutable.Map(firstMapWithIndex.map(_._2._1 -> 0).toSeq: _*)
    val newTree = new Node[String](null, List())
    val (newFlag, nNodi) = creazioneAlberoItem(newTree, pathsSorted, flag = false, 0, counterDifferentNode, firstMapWithIndex)

    if (!newFlag) {
      if (newTree.sons.nonEmpty) {
        val itemsFreq = itemMapSorted.toSet
        //Vegono create tutte le possibili combinazioni tra gli item frequenti
        val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
        //Vengono ottenuti i frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
        subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item)._2)
      } else {
        List(List(item) -> firstMapSorted(item)._2)
      }
    }
    else {
      //inizializzazione dell'array in cui vengono contenuti gli indici, che indicano da dove iniziano le celle contigue per ogni item nel trie
      val startIndex = new Array[Int](itemMapSorted.size)
      val newArrayTrie = new Array[(String, Int, Int)](nNodi)
      calcSI(startIndex, counterDifferentNode)

      createTrie2(newArrayTrie, newTree, startIndex, firstMapWithIndex, -1)

      val conditionalPatternBase = createConditionalPatternBase(newArrayTrie, newArrayTrie.length - 1, ListMap.empty[String, List[(List[String], Int)]])
      val freqIS = createFreqItemSet(conditionalPatternBase, firstMapWithIndex, List[(List[String], Int)]())
      freqIS.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item)._2)
    }
  }


  def calcSI(startIndex: Array[Int], counterDifferentNode: mutable.Map[Int, Int]): Unit = {
    for (i <- 1 until startIndex.length) {
      startIndex(i) = counterDifferentNode(i - 1) + startIndex(i - 1)
    }
  }

  @tailrec
  def putElementinArray(arrayTrie: Array[(String, Int, Int)], index: Int, node: Node[String], parent: Int): Int = {
    if (arrayTrie(index) == null) {
      arrayTrie(index) = (node.value, node.occurrence, parent)
      index
    } else
      putElementinArray(arrayTrie, index + 1, node, parent)
  }


  @tailrec
  def createTrieSons2(arrayTrie: Array[(String, Int, Int)], sons: List[Node[String]], startIndex: Array[Int],
                      firstMapWithIndex: Map[String, (Int, Int)], parentIndex: Int): Unit = {

    if (sons.nonEmpty) {
      val firstSon = sons.head
      createTrie2(arrayTrie, firstSon, startIndex, firstMapWithIndex, parentIndex)
      createTrieSons2(arrayTrie, sons.tail, startIndex, firstMapWithIndex, parentIndex)
    }
  }

  def createTrie2(arrayTrie: Array[(String, Int, Int)], lastNode: Node[String], startIndex: Array[Int],
                  firstMapWithIndex: Map[String, (Int, Int)], parentIndex: Int): Unit = {
    if (lastNode.isHead) {
      createTrieSons2(arrayTrie, lastNode.sons, startIndex, firstMapWithIndex, parentIndex)
    } else {
      val index = firstMapWithIndex(lastNode.value)._1
      val firstIndex = startIndex(index)
      val newParent = putElementinArray(arrayTrie, firstIndex, lastNode, parentIndex)
      createTrieSons2(arrayTrie, lastNode.sons, startIndex, firstMapWithIndex, newParent)
    }

  }


  def exec() = {

    //Calcolo della frequenza dei singoli items
    val firstStep = time(countItemSet(totalItem).filter(x => x._2 >= minSupport))

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2))
    //firstMapSorted.foreach(println)

    //Per ogni item creiamo l'indice così da facilitarci l'ordinamento più avanti
    // String -> indice, frequenza
    val firstMapWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2)).toMap

    //firstMapWithIndex.foreach(println)

    //Creiamo il nostro albero vuoto
    val newTree = new Node[String](null, List())

    val counterDifferentNode = mutable.Map(firstMapWithIndex.map(_._2._1 -> 0).toSeq: _*)

    //Scorriamo tutte le transazioni creando il nostro albero
    val numeroNodi = time(creazioneAlbero(newTree, dataset, firstMapWithIndex, 0, counterDifferentNode))

    //inizializzazione dell'array in cui vengono contenuti gli indici, che indicano da dove iniziano le celle contigue per ogni item nel trie
    //val startIndex = new Array[Int](firstMapWithIndex.size).map(x => -1) //Vengono inizializzate tutte a -1
    val startIndex2 = new Array[Int](firstMapWithIndex.size)
    calcSI(startIndex2, counterDifferentNode)
    //creazione del trie, ogni elemento della list (valore, frequenza, genitore) -> vedere con tutti se bisogna lasciare il valore, secondo me sì
    println("Array iniziale! ")
    //val trie = time(createTrie(newTree, -1, startIndex, List.empty[(String, Int, Int)], firstMapWithIndex))

    val newArrayTrie = new Array[(String, Int, Int)](numeroNodi)

    //printTree(newTree, "")

    /*


        println("StartIndex Calcolato" + startIndex.toList)
        println("StartIndex2 Calcolato" + startIndex2.toList)
        println(trie)
        println(numeroNodi)

        println(counterDifferentNode)*/

    time(createTrie2(newArrayTrie, newTree, startIndex2, firstMapWithIndex, -1))

    // println("new ArrayTrie: " + newArrayTrie.toList)


    /*val index = startIndex(firstMapWithIndex("b")._1)
    val indexSucc = startIndex(firstMapWithIndex("b")._1 + 1)

    println(index)
    println(indexSucc)

    val listaNodiB = trie.slice(index, indexSucc)

    println(listaNodiB)*/

    //Creazionde del conditionalPatternBase scorrendo il trie(la lista con i nodi)
    println("Conditional PatternBase! ")
    val conditionalPatternBase = time(createConditionalPatternBase(newArrayTrie, newArrayTrie.length - 1,
      ListMap.empty[String, List[(List[String], Int)]]))

    //conditionalPatternBase.foreach(println)

    println("Ciao!!")
    val frequentItemSet = time(createFreqItemSet(conditionalPatternBase, firstMapWithIndex, List[(List[String], Int)]()))
    frequentItemSet.map(x => x._1.toSet -> x._2).toMap

  }

  //println(dataset.length)
  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "NonOrderFpResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "NonOrderFpSupport.txt")

}
