import Utils._
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable

object FPGrowthRDD extends App {
  val sc = Utils.getSparkContext("FPGrowthRDD")
  val lines = Utils.getRDD("datasetKaggleAlimenti.txt", sc)
  val dataset = lines.map(x => x.split(","))
  val dataset3 = sc.parallelize(
    List(Array("a", "c", "d", "f", "g", "i", "m", "p")
      , Array("a", "b", "c", "f", "i", "m", "o")
      , Array("b", "f", "h", "j", "o")
      , Array("b", "c", "k", "s", "p")
      , Array("a", "c", "e", "f", "l", "m", "n", "p")))

  val numParts = 10

  def getSingleItemCount(partitioner: Partitioner): Array[(String, Int)] = {
    dataset.flatMap(t => t).map(v => (v, 1))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= Utils.minSupport)
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
  def addNodeTransaction(lastNode: Node[String], transazione: Array[String], headerTable: ListMap[String, List[Node[String]]]): ListMap[String, List[Node[String]]] = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => List[Node[String]]() //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (transazione.head -> (old :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodeTransaction(node._1, transazione.tail, newTable)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodeTransaction(node._1, transazione.tail, headerTable)
      }
    } else {
      //Quando finisce una singola transazione
      headerTable
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

  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: List[Array[String]], headerTable: ListMap[String, List[Node[String]]], itemToRank: Map[String, Int]): ListMap[String, List[Node[String]]] = {
    if (transactions.nonEmpty) {
      val head = transactions.head //Singola transazione
      val newHeaderTable = addNodeTransaction(tree, head, headerTable) //Ricorsivo su tutta la transazione
      creazioneAlbero(tree, transactions.tail, newHeaderTable, itemToRank) //Una volta aggiunta una transazione continuiamo con le successive
    } else headerTable //Finite tutte le transazioni del dataset restituiamo l'ht
  }

  //Risaliamo l'albero per restituire il percorso inerente ad un nodo specifico
  @tailrec
  def listaPercorsi(nodo: Node[String], listaPercorsoAcc: List[String]): List[String] = {
    if (!nodo.padre.isHead) //Se non è il primo nodo
      listaPercorsi(nodo.padre, nodo.padre.value :: listaPercorsoAcc) //Continuiamo a risalire l'albero col padre
    else
      listaPercorsoAcc //Restituiamo tutto il percorso trovato
  }

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

  def firstStep(partitioner: HashPartitioner): Array[(String, Int)] = {
    //First step
    val singleItemsCount = getSingleItemCount(partitioner)
    singleItemsCount
  }

  private def genCondTransactions(transaction: Array[String],
                                  itemToRank: Map[String, Int],
                                  partitioner: Partitioner): mutable.Map[Int, Array[String]] = {
    val output = mutable.Map.empty[Int, Array[String]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.filter(item => itemToRank.contains(item)).sortBy(item => itemToRank(item))
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }

  def countSummary(list: List[Node[String]]): Int = {
    list.foldLeft(0)((x, y) => x + y.occurrence)
  }

  //Creazione Conditional FPTree per un singolo item
  @tailrec
  def creazioneAlberoItem(tree: Node[String], sortedPaths: List[(List[String], Int)],
                          headerTable: ListMap[String, List[Node[String]]]):
  ListMap[String, List[Node[String]]] = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      val newHeaderTable = addNodePath(tree, head._1, head._2, headerTable)
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, sortedPaths.tail, newHeaderTable)
    } else headerTable //Esaminati tutti i path restituiamo ht e il flag dei branch
  }

  //Aggiungiamo i nodo di un path all'albero
  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int,
                  headerTable: ListMap[String, List[Node[String]]]):
  ListMap[String, List[Node[String]]] = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val node = lastNode.add(path.head, countPath)

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(path.head) match {
          case Some(value) => value
          case None => (List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (path.head -> (old :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodePath(node._1, path.tail, countPath, newTable)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodePath(node._1, path.tail, countPath, headerTable)
      }
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituita l' ht e il flag relativo alla formazione di nuovi branch
      headerTable
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

  def totalItem(listPaths: List[(List[String], Int)]) = (listPaths.foldLeft(Set[String]())((xs, x) => xs ++ x._1)).toList

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(headerTable: ListMap[String, List[Node[String]]],
              validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {
    headerTable.iterator.flatMap { case (item, summary) =>
      val summaryCount = countSummary(summary)
      if (validateSuffix(item) && summaryCount >= minSupport) {
        val lPerc = headerTable(item).map(x => (listaPercorsi(x, List[String]()), x.occurrence))
        val condTree = new Node[String](null, List())

        //creazione header table
        val headerTableItem = ListMap(totalItem(lPerc).map(x => x -> List[Node[String]]()): _*)
        val newHT = creazioneAlberoItem(condTree, lPerc, headerTableItem)

        Iterator.single((item :: Nil, summaryCount)) ++ extract(newHT).
          map { case (t, c) => (item :: t, c) }
      } else {
        Iterator.empty
      }
    }
  }

  def exec(): Map[Set[String], Int] = {

    val partitioner = new HashPartitioner(numParts)
    //totalItems che rispettano il minSupport
    val firstStepVar = firstStep(partitioner)
    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStepVar.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    val itemToRank = firstMapSorted.keys.zipWithIndex.toMap

    val app = dataset.flatMap(transaction =>
      genCondTransactions(transaction, itemToRank, partitioner))

    val app2 = app.groupByKey(partitioner.numPartitions).
      map(x => x._1 -> {
        val tree = new Node[String](null, List())
        creazioneAlbero(tree, x._2.toList, firstMapSorted.map(x => x._1 -> (List[Node[String]]())), itemToRank).filter(_._2.nonEmpty)
      }
      )

    val app4 = app2.flatMap(elem => extract(elem._2, x => partitioner.getPartition(x) == elem._1))

    app4.map(x => x._1.toSet -> x._2).collect().toMap

  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthRDDResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthRDDResultSupport.txt")
}