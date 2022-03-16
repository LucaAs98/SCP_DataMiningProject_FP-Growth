package fpgrowthold

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import utils.Utils._
import classes.Node
import mainClass.MainClass.minSupport

object NonordFPRDDOld extends App {
  val sc = getSparkContext("NonordFPRDDOld")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val (lines, dimDataset) = getRDD(sc)
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

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction(lastNode: Node[String], transazione: Array[String], count: Int, itemMap: Map[String, (Int, Int)], counterDifferentNode: mutable.Map[Int, Int]): Int = {
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

  //Creazione dell'albero
  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: Iterable[Array[String]], itemMap: Map[String, (Int, Int)], count: Int, counterDifferentNode: mutable.Map[Int, Int]): Int = {
    if (transactions.nonEmpty) {
      val head = transactions.head //Singola transazione
      //Aggiungiamo i nodi della transazione all'albero, contiamo quanti nodi nuovi abbiamo aggiunto
      val newCount = addNodeTransaction(tree, head, count, itemMap, counterDifferentNode)
      //Una volta aggiunta una transazione continuiamo con le successive
      creazioneAlbero(tree, transactions.tail, itemMap, newCount, counterDifferentNode)
    }
    else
      count
  }

  //Risaliamo l'albero per restituire il percorso inerente ad un nodo specifico
  @tailrec
  def listaPercorsi(nodo: Node[String], listaPercorsoAcc: List[String]): List[String] = {
    if (!nodo.padre.isHead) //Se non è il primo nodo
      listaPercorsi(nodo.padre, nodo.padre.value :: listaPercorsoAcc) //Continuiamo a risalire l'albero col padre
    else
      listaPercorsoAcc //Restituiamo tutto il percorso trovato
  }


  //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
  def genCondTransactions(transaction: Array[String],
                          itemToRank: ListMap[String, (Int, Int)],
                          partitioner: Partitioner): mutable.Map[Int, Array[String]] = {
    //Mappa finale da riempire
    val output = mutable.Map.empty[Int, Array[String]]
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
        output(part) = transFiltered.slice(0, i + 1)
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

  //Creazione di un percorso dato un elemento, simile a listaPercorsi, ma per l'arrayTrie
  @tailrec
  def getPercorso(parent: Int, arrayTrie: Array[(String, Int, Int)], acc: List[String]): List[String] = {
    if (parent != -1) {
      val last = arrayTrie(parent)
      getPercorso(last._3, arrayTrie, (last._1) :: acc)
    } else {
      List.empty[String] ::: acc
    }
  }

  //Creazione dell'albero per il singolo item, simile alla creazione dell'albero iniziale
  @tailrec
  def creazioneAlberoItem(tree: Node[String], sortedPaths: Array[(List[String], Int)], count: Int,
                          counterDifferentNode: mutable.Map[Int, Int], itemMap: Map[String, (Int, Int)]): Int = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      val newCount = addNodePath(tree, head._1, head._2, count, counterDifferentNode, itemMap)
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, sortedPaths.tail, newCount, counterDifferentNode, itemMap)
    } else count //Esaminati tutti i path restituiamo il flag dei branch ed il numero di nodi distinti
  }

  //Aggiungiamo il singolo nodo per il path (Siamo nella creazione dell'albero per il singolo item)
  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int, count: Int,
                  counterDifferentNode: mutable.Map[Int, Int], itemMap: Map[String, (Int, Int)]): Int = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val (node, flagNewNode) = lastNode.add(path.head, countPath)

      //Contiamo i nodi distinti
      val newCount = count + {
        if (flagNewNode) {
          counterDifferentNode(itemMap(node.value)._1) += 1
          1
        } else 0
      }

      //Continuiamo a scorrere il path
      addNodePath(node, path.tail, countPath, newCount, counterDifferentNode, itemMap)
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituito il flag relativo alla formazione di nuovi branch ed il count dei nodi distinti
      count
    }
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
  def createFreqItemSet(startIndex: Array[Int], arrayTrie: Array[(String, Int, Int)],
                        countDiffNode: mutable.Map[Int, Int], indexToItem: Map[Int, String],
                        validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {


    countDiffNode.iterator.flatMap {
      case (indexItem, itemDiffNode) =>
        val stringItem = indexToItem(indexItem)
        val nodesItem = arrayTrie.slice(startIndex(indexItem), startIndex(indexItem) + itemDiffNode)


        val frequenzaItem = countItemNodeFreq(nodesItem)

        /* Controlliamo che quel determinato item sia della partizione che stiamo considerando e quelli sotto il min supp
        * (Dopo il primo passaggio validateSuffix, sarà sempre true). */
        if (validateSuffix(stringItem) && frequenzaItem >= minSupport) {
          //Prendiamo tutti i percorsi per quel determinato item senza quest'ultimo
          val lPerc = nodesItem.map(nodo => getPercorso(nodo._3, arrayTrie, List[String]()) -> nodo._2)

          //Contiamo quante volte esiste un singolo item all'interno di tutti i path nel condPB di un determinato item e rimuoviamo quelli sotto il min supp
          val itemCount = countItemConPB(lPerc, Map.empty[String, Int]) //.filter(x => x._2 >= minSupport)

          //Ordiniamo itemCount
          //val itemMapSorted = ListMap(itemCount.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

          //Rimuoviamo dai path che portano all'item tutti gli elementi sotto il min supp e li riordiniamo in base ad itemMapSorted
          //val pathsSorted = condPBSingSort(lPerc, itemMapSorted.keys.toList)

          //Aggiungiamo l'indice ad itemMapSorted per facilitarci riordinamenti successivi
          val firstMapWithIndexItem = itemCount.zipWithIndex.map(x => x._1._1 -> (x._2, x._1._2))

          val itemToRankItem = firstMapWithIndexItem.map(elem => elem._2._1 -> elem._1)

          //Mappa che contiene tutti i nodi dell'albero distinti (anche i nodi con stesso item, ma in diversa posizione)
          val countDiffNodeItem = mutable.Map(firstMapWithIndexItem.map(_._2._1 -> 0).toSeq: _*)

          //Continuiamo a calcolare i conditional trees 'interni'
          val condTree = new Node[String](null, List())

          //Creiamo l'albero condizionale dell'item e riempiamo counterDifferentNode
          val numNodiItem = creazioneAlberoItem(condTree, lPerc, 0, countDiffNodeItem, firstMapWithIndexItem)

          //Ricalcoliamo startIndex e l'arrayTrie
          val startIndexItem = new Array[Int](firstMapWithIndexItem.size)
          val arrayTrieItem = new Array[(String, Int, Int)](numNodiItem)
          calcStartIndex(startIndexItem, countDiffNodeItem)
          createTrie(arrayTrieItem, condTree, startIndexItem, firstMapWithIndexItem, -1)

          //Creazione dei freqItemSet
          Iterator.single((stringItem :: Nil, frequenzaItem)) ++ createFreqItemSet(startIndexItem, arrayTrieItem, countDiffNodeItem, itemToRankItem).map {
            case (t, c) => (stringItem :: t, c)
          }
        } else {
          //Se non fa parte di quel gruppo restituiamo iterator vuoto
          Iterator.empty
        }
    }
  }

  def createCondTrees(transactionsGroup: Iterable[Array[String]], itemToRank: ListMap[String, (Int, Int)]) = {
    //Creiamo il nostro albero vuoto
    val tree = new Node[String](null, List())
    //Mappa che contiene tutti i nodi dell'albero distinti (anche i nodi con stesso item, ma in diversa posizione)
    val counterDifferentNode = mutable.Map(itemToRank.map(_._2._1 -> 0).toSeq: _*)

    //Scorriamo tutte le transazioni creando il nostro albero. Restituiamo il numero di nodi distinti
    val numeroNodi = creazioneAlbero(tree, transactionsGroup, itemToRank, 0, counterDifferentNode)

    (numeroNodi, tree, counterDifferentNode)
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

  def createArrayTrie(itemToRank: ListMap[String, (Int, Int)], counterDifferentNode: mutable.Map[Int, Int], numeroNodi: Int, tree: Node[String]) = {
    /* Inizializzazione dell'array in cui sono contenuti gli indici, che indicano da dove iniziano le celle contigue
    * per ogni item nell'arrayTrie. */
    val startIndex = new Array[Int](itemToRank.size)

    //Calcoliamo startIndex partendo dal conto dei nodi di un certo item
    calcStartIndex(startIndex, counterDifferentNode)

    //Inizializziamo l'arrayTrie con il numero di nodi calcolati in precedenza
    val arrayTrie = new Array[(String, Int, Int)](numeroNodi)

    //Riempiamo l'arrayTrie
    createTrie(arrayTrie, tree, startIndex, itemToRank, -1)

    (startIndex, arrayTrie)
  }

  //Esecuzione effettiva dell'algoritmo
  def exec() = {
    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }

  def avviaAlgoritmo():Map[Set[String], Int] = {
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

    //condTransGrouped.map(elem => elem._1 -> elem._2.map(x => x.toList).toList).foreach(println)


    val condArrayStartTrie = condTransGrouped.map(group =>
      group._1 -> {
        //println("\n---------------------\n" + group._1)
        val (numNodiGroup, treeGroup, countDiffNodeGroup) = createCondTrees(group._2, itemToRank)
        //printTree(treeGroup, "")

        val newItemToRank = itemToRank.filter(x => countDiffNodeGroup(x._2._1) > 0).zipWithIndex.map(elem => elem._1._1 -> (elem._2, elem._1._2._2))
        val newCDNG = mutable.Map(countDiffNodeGroup.filter(_._2 > 0).toList.sortBy(_._1).zipWithIndex.map(elem => elem._2 -> elem._1._2): _*)
        val (startIndexGroup, arrayTrieGroup) = createArrayTrie(newItemToRank, newCDNG, numNodiGroup, treeGroup)
        val indexToItem = newItemToRank.map(elem => elem._2._1 -> elem._1)
        //arrayTrieGroup.toList.foreach(println)
        (startIndexGroup, arrayTrieGroup, newCDNG, indexToItem)
      })


    //Caloliamo i frequentItemSet
    val freqItemSet = condArrayStartTrie.flatMap(elem => createFreqItemSet(elem._2._1, elem._2._2, elem._2._3, elem._2._4, x => partitioner.getPartition(x) == elem._1))

    freqItemSet.map(x => x._1.toSet -> x._2).collect().toMap
  }
}
