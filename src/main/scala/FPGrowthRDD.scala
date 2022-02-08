import Utils._
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

object FPGrowthRDD extends App {
  val sc = Utils.getSparkContext("FPGrowthRDD")
  val lines = Utils.getRDD("datasetKaggleAlimenti100.txt", sc)
  val dataset = lines.map(x => x.split(","))
  val dataset2 = sc.parallelize(
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
  def addNodeTransaction(lastNode: Node[String], transazione: Array[String], headerTable: ListMap[String, (Int, List[Node[String]])]): ListMap[String, (Int, List[Node[String]])] = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => (0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (transazione.head -> (old._1, old._2 :+ node._1))

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
  def creazioneAlbero(tree: Node[String], transactions: Array[Array[String]], headerTable: ListMap[String, (Int, List[Node[String]])], itemToRank: Map[String, Int]): ListMap[String, (Int, List[Node[String]])] = {
    if (transactions.nonEmpty) {
      val head = transactions.head //Singola transazione
      val headOrdinata = head.filter(item => itemToRank.contains(item)).sortBy(item => itemToRank(item))
      val newHeaderTable = addNodeTransaction(tree, headOrdinata, headerTable) //Ricorsivo su tutta la transazione
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


  def firstStep(): Array[(String, Int)] = {
    val partitioner = new HashPartitioner(numParts)

    //First step
    val singleItemsCount = getSingleItemCount(partitioner)
    singleItemsCount
  }

  def exec(): Map[Set[String], Int] = {
    //totalItems che rispettano il minSupport
    val firstStepVar = firstStep()
    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStepVar.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    val itemToRank = firstMapSorted.keys.zipWithIndex.toMap

    //Creiamo il nostro albero vuoto
    val newTree = new Node[String](null, List())

    //Accumulatore per tutta l'headerTable
    val headerTable = firstMapSorted.map(x => x._1 -> (x._2, List[Node[String]]()))

    //Scorriamo tutte le transazioni creando il nostro albero e restituendo l'headerTable finale
    val headerTableFinal = creazioneAlbero(newTree, dataset.collect(), headerTable, itemToRank)

    //printTree(newTree, "")

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

    //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
    val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> headerTableFinal(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence)))

    //Calcoliamo il nostro risultato finale
    val frequentItemSet = conditionalPatternBase.flatMap(elem => itemSetFromOne(elem._1, elem._2, Map[Set[String], Int]())).filter(_._2 >= minSupport)

    frequentItemSet
  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultSupport.txt")
}