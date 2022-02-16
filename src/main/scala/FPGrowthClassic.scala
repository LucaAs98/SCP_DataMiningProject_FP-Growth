import Utils._

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

object FPGrowthClassic extends App {
  val dataset = Utils.prendiDataset("datasetKaggleAlimenti.txt")
  val dataset2 =
    List(Set("a", "c", "d", "f", "g", "i", "m", "p")
      , Set("a", "b", "c", "f", "i", "m", "o")
      , Set("b", "f", "h", "j", "o")
      , Set("b", "c", "k", "s", "p")
      , Set("a", "c", "e", "f", "l", "m", "n", "p"))

  val totalItem = (dataset reduce ((xs, x) => xs ++ x)).toList //Elementi singoli presenti nel dataset

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[String]): Map[String, Int] = {
    (item map (x => x -> (dataset count (y => y.contains(x))))).toMap
  }

  //
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

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction(lastNode: Node[String], transazione: List[String], headerTable: ListMap[String, (Int, List[Node[String]])]): ListMap[String, (Int, List[Node[String]])] = {
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
  def creazioneAlbero(tree: Node[String], transactions: List[List[String]], headerTable: ListMap[String, (Int, List[Node[String]])]): ListMap[String, (Int, List[Node[String]])] = {
    if (transactions.nonEmpty) {
      val head = transactions.head //Singola transazione
      val newHeaderTable = addNodeTransaction(tree, head, headerTable) //Ricorsivo su tutta la transazione
      creazioneAlbero(tree, transactions.tail, newHeaderTable) //Una volta aggiunta una transazione continuiamo con le successive
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

  def calcoloMinimi(elemento: Set[(String, Int)]) = {
    if (elemento.size > 1) {
      elemento.tail.foldLeft((List(elemento.head._1), elemento.head._2))((x, y) => (x._1 :+ y._1, x._2.min(y._2)))
    } else {
      (List(elemento.head._1), elemento.head._2)
    }
  }

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction2(lastNode: Node[String], transazione: List[String], countTrans: Int,
                          headerTable: ListMap[String, (Int, List[Node[String]])], flag: Boolean):
  (ListMap[String, (Int, List[Node[String]])], Boolean) = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head, countTrans)

      val newFlag = {
        if (!flag) lastNode.sons.size > 1
        else flag
      }

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => (0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (transazione.head -> (old._1, old._2 :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodeTransaction2(node._1, transazione.tail, countTrans, newTable, newFlag)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodeTransaction2(node._1, transazione.tail, countTrans, headerTable, newFlag)
      }
    } else {
      //Quando finisce una singola transazione
      (headerTable, flag)
    }
  }


  @tailrec
  def creazioneAlberoItem(tree: Node[String], orderEs: List[(List[String], Int)],
                          headerTable: ListMap[String, (Int, List[Node[String]])], flag: Boolean):
  (ListMap[String, (Int, List[Node[String]])], Boolean) = {
    if (orderEs.nonEmpty) {
      val head = orderEs.head //Singola transazione
      val (newHeaderTable, newFlag) = addNodeTransaction2(tree, head._1, head._2, headerTable, flag) //Ricorsivo su tutta la transazione
      creazioneAlberoItem(tree, orderEs.tail, newHeaderTable, newFlag) //Una volta aggiunta una transazione continuiamo con le successive
    } else (headerTable, flag) //Finite tutte le transazioni del dataset restituiamo l'ht
  }


  def condPBSingSort(primo: List[(List[String], Int)], elementSorted: List[String]) = {
    primo.map(elem => elem._1.filter(elementSorted.contains).sortBy(elementSorted.indexOf(_)) -> elem._2)
  }


  @tailrec
  def countItemConPB(liste: List[(List[String], Int)], acc: Map[String, Int]): Map[String, Int] = {

    if (liste.nonEmpty) {
      val head = liste.head
      val subMap = head._1.map(x => x -> head._2).toMap
      val subMapFinal = acc ++ subMap.map { case (k, v) => k -> (v + acc.getOrElse(k, 0)) }
      countItemConPB(liste.tail, subMapFinal)
    }
    else
      acc
  }

  def freqItemsetCondPB(item: String, headList: List[(List[String], Int)], firstMapSorted: ListMap[String, Int]): List[(List[String], Int)] = {
    val itemCount = countItemConPB(headList, Map.empty[String, Int]).filter(x => x._2 >= minSupport)
    val itemMapSorted = ListMap(itemCount.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    val orderedPath = condPBSingSort(headList, itemMapSorted.keys.toList)
    val headerTableItem = itemMapSorted.map(x => x._1 -> (x._2, List[Node[String]]()))
    val condTreeItem = new Node[String](null, List())
    val (headerTableItemFin, moreBranch) = creazioneAlberoItem(condTreeItem, orderedPath, headerTableItem, flag = false)


    if (!moreBranch) {
      //printTree(condTreeItem, "")

      if (headerTableItemFin.nonEmpty) {
        val itemsFreq = headerTableItemFin.map(x => x._1 -> x._2._1).toSet
        val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
        subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item))
      }
      else {
        List((List(item) -> firstMapSorted(item)))
      }
    } else {
      val itemCresOrder = ListMap(itemMapSorted.toList.reverse: _*)
      val newCondPB = itemCresOrder.map(x => x._1 -> headerTableItemFin(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence)))
      val freqItemsetItem = condFPTree(newCondPB, itemMapSorted, List[(List[String], Int)]())
      freqItemsetItem.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item))
    }
  }

  @tailrec
  def condFPTree(conditionalPatternBase: ListMap[String, List[(List[String], Int)]], firstMapSorted: ListMap[String, Int],
                 accFreqItemset: List[(List[String], Int)]): List[(List[String], Int)] = {
    if (conditionalPatternBase.nonEmpty) {
      val (item, headList) = conditionalPatternBase.head
      val freqItemset = freqItemsetCondPB(item, headList, firstMapSorted)
      condFPTree(conditionalPatternBase.tail, firstMapSorted, accFreqItemset ++ freqItemset)
    }
    else {
      accFreqItemset
    }
  }


  def exec() = {
    //totalItems che rispettano il minSupport

    //
    // println(dataset.length)

    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    //firstStep.foreach(println)
    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    //firstMapSorted.foreach(println)
    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList)

    //Creiamo il nostro albero vuoto
    val newTree = new Node[String](null, List())

    //Accumulatore per tutta l'headerTable
    val headerTable = firstMapSorted.map(x => x._1 -> (x._2, List[Node[String]]()))

    //Scorriamo tutte le transazioni creando il nostro albero e restituendo l'headerTable finale
    val headerTableFinal = creazioneAlbero(newTree, orderDataset, headerTable)

    //printTree(newTree, "")

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

    //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
    val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> headerTableFinal(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence)))

    //conditionalPatternBase.foreach(println)

    val allFreqitemset = condFPTree(conditionalPatternBase, firstMapSorted, List[(List[String], Int)]())

    //allFreqitemset.foreach(println)

    allFreqitemset.map(x => x._1.toSet -> x._2).toMap

    //Inizio sperimentazione!!!!!!!!
    /*
        val primo = conditionalPatternBase("m")

        val es = countItemConPB(primo, Map.empty[String, Int]).filter(x => x._2 >= minSupport)

        es.foreach(println)
        val firstMapSorted2 = time(ListMap(es.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*))

        firstMapSorted2.foreach(println)

        val orderEs = condPBSingSort(primo, firstMapSorted2.keys.toList)

        orderEs.sortBy(_._1.length).foreach(println)

        val headerTable2 = firstMapSorted2.map(x => x._1 -> (x._2, List[Node[String]]()))
        val newTree2 = new Node[String](null, List())

        val (headerTable2Fin, flag) = creazioneAlberoItem(newTree2, orderEs, headerTable2, false)

        printTree(newTree2, "")
        println("acacascac")
        headerTable2Fin.foreach(println)
        println(flag)

        val itemsFreq = headerTable2Fin.map(x => x._1 -> x._2._1).toSet

        val subsetsitem = itemsFreq.subsets().filter(_.nonEmpty).toSet

        subsetsitem.foreach(println)

        val itemsetFreq = subsetsitem.map(set => calcoloMinimi(set)).map(x => (x._1 :+ "m") -> x._2) + (List("m") -> firstMapSorted("m"))

        println("ItemsetFreq")
        itemsetFreq.foreach(println)*/

    //Calcoliamo il nostro risultato finale
    //val frequentItemSet = conditionalPatternBase.flatMap(elem => time(itemSetFromOne(elem._1, elem._2, Map[Set[String], Int]()))).filter(_._2 >= minSupport)

  }

  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthClassicResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultClassicSupport.txt")

}
