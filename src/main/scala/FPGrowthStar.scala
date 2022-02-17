import Utils._

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

object FPGrowthStar extends App {
  val dataset = Utils.prendiDataset("datasetKaggleAlimenti.txt")
  val dataset2 =
    List(Set("a", "c", "d", "f", "g", "i", "m", "p")
      , Set("a", "b", "c", "f", "i", "m", "o")
      , Set("b", "f", "h", "j", "o")
      , Set("b", "c", "k", "s", "p")
      , Set("a", "c", "e", "f", "l", "m", "n", "p"))
  val dataset3 =
    List(Set("a", "b", "c", "e", "f", "o")
      , Set("a", "c", "g")
      , Set("e", "i")
      , Set("a", "c", "d", "e", "g")
      , Set("a", "c", "e", "g", "l")
      , Set("e", "j")
      , Set("a", "b", "c", "e", "f", "p")
      , Set("a", "c", "d")
      , Set("a", "c", "e", "g", "m")
      , Set("a", "c", "e", "g", "n"))

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

  def aggiornaMatrice(matrice: ListMap[String, Array[Int]], listaPrecedenti: List[Int], item: String, count: Int): Unit = {
    val array = matrice(item)
    listaPrecedenti.foreach(index => array(index) += count)
  }

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction(lastNode: Node[String], transazione: List[String],
                         headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                         matrice: ListMap[String, Array[Int]],
                         listaPrecedenti: List[Int]):
  ListMap[String, (Int, Int, List[Node[String]])] = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)

      if (listaPrecedenti.nonEmpty) {
        aggiornaMatrice(matrice, listaPrecedenti, transazione.head, 1)
      }

      val newListaPrecedenti = listaPrecedenti :+ headerTable(transazione.head)._2

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => (0, 0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (transazione.head -> (old._1, old._2, old._3 :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodeTransaction(node._1, transazione.tail, newTable, matrice, newListaPrecedenti)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodeTransaction(node._1, transazione.tail, headerTable, matrice, newListaPrecedenti)
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
  def creazioneAlbero(tree: Node[String], transactions: List[List[String]],
                      headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                      matrice: ListMap[String, Array[Int]]):
  ListMap[String, (Int, Int, List[Node[String]])] = {
    if (transactions.nonEmpty) {
      val head = transactions.head //Singola transazione
      val newHeaderTable = addNodeTransaction(tree, head, headerTable, matrice, List[Int]()) //Ricorsivo su tutta la transazione
      creazioneAlbero(tree, transactions.tail, newHeaderTable, matrice) //Una volta aggiunta una transazione continuiamo con le successive
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
                          headerTable: ListMap[String, (Int, Int, List[Node[String]])], flag: Boolean,
                          matrice: ListMap[String, Array[Int]],
                          listaPrecedenti: List[Int]):
  (ListMap[String, (Int, Int, List[Node[String]])], Boolean) = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head, countTrans)

      val newFlag = {
        if (!flag) lastNode.sons.size > 1
        else flag
      }

      val newListaPrecedenti = {
        if (matrice.nonEmpty) {
          if (listaPrecedenti.nonEmpty) {
            aggiornaMatrice(matrice, listaPrecedenti, transazione.head, countTrans)
          }
          listaPrecedenti :+ headerTable(transazione.head)._2
        } else {
          listaPrecedenti
        }
      }

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => (0, 0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (transazione.head -> (old._1, old._2, old._3 :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodeTransaction2(node._1, transazione.tail, countTrans, newTable, newFlag, matrice, newListaPrecedenti)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodeTransaction2(node._1, transazione.tail, countTrans, headerTable, newFlag, matrice, newListaPrecedenti)
      }
    } else {
      //Quando finisce una singola transazione
      (headerTable, flag)
    }
  }


  def creaMatrice(listSorted: ListMap[String, (Int, Int)]) = {
    if (listSorted.size > 1) {
      listSorted.tail.map(elem => elem._1 -> new Array[Int](elem._2._2).map(x => 0))
    }
    else
      ListMap.empty[String, Array[Int]]
  }

  def exec() = {


    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    //firstStep.foreach(println)
    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    //firstMapSorted.foreach(println)
    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList)

    //Creiamo il nostro albero vuoto
    val newTree = new Node[String](null, List())


    //Item -> (Frequenza, Indice)
    val firstMapSortedWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2))
    val firstMapSortedWithIndexContrario = firstMapSortedWithIndex.map(elem => elem._2._2 -> (elem._1))
    //Accumulatore per tutta l'headerTable
    val headerTable = firstMapSortedWithIndex.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))

    val matrix = creaMatrice(firstMapSortedWithIndex)

    //Scorriamo tutte le transazioni creando il nostro albero e restituendo l'headerTable finale
    val headerTableFinal = creazioneAlbero(newTree, orderDataset, headerTable, matrix)

    printTree(newTree, "")
    val allFreqitemset = calcFreqItemset(headerTableFinal.keys.toList, headerTableFinal, matrix,List[(List[String], Int)]())

    allFreqitemset.map(x => x._1.toSet -> x._2).toMap


    /*


        //inizio simulazione!!!!!!!!!!!!!!!!!!!!!!!!

        println(matrix.map(elem => elem._1 -> elem._2.toList).mkString("\n"))


        val g = matrix("g").toList

        println(g)

        val gWithItem = g.zipWithIndex.map(x => firstMapSortedWithIndexContrario(x._2) -> x._1)
        println(gWithItem)

        val gSorted = ListMap(gWithItem.filter(_._2 >= minSupport).sortWith((elem1, elem2) => functionOrder(elem1, elem2)).zipWithIndex
          .map(x => x._1._1 -> (x._1._2, x._2)): _*)
        println(gSorted)

        val gLinkedList = headerTableFinal("g")._3
        println(gLinkedList)

        val firstG = gLinkedList.last

        val percorsoUno = listaPercorsi(firstG, List[String]())

        println(percorsoUno)


        //controllo grandezza gSorted (==1)
        val matrix2 = creaMatrice(gSorted.tail)
        println(matrix2.map(elem => elem._1 -> elem._2.toList).mkString("\n"))
        val headerTable2 = gSorted.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))
        val newtree2 = new Node[String](null, List())

        val percorsoUnoOrdinato = (percorsoUno.filter(x => gSorted.contains(x)).sortBy(elem => gSorted(elem)._2), firstG.occurrence)

        println(percorsoUnoOrdinato)

        val (newHT, moreBranch) = addNodeTransaction2(newtree2, percorsoUnoOrdinato._1, percorsoUnoOrdinato._2, headerTable2,
          flag = false, matrix2, List[Int]())

        println(newHT)
        printTree(newtree2, "")
        println(matrix2.map(elem => elem._1 -> elem._2.toList).mkString("\n"))


        //Seocndo passaggio
        val secondG = gLinkedList.head
        val percorsoDue = listaPercorsi(secondG, List[String]())
        println(percorsoDue)

        val percorsoDueOrdinato = (percorsoDue.filter(x => gSorted.contains(x)).sortBy(elem => gSorted(elem)._2), secondG.occurrence)
        println(percorsoDueOrdinato)


        val (newHT2, moreBranch2) = addNodeTransaction2(newtree2, percorsoDueOrdinato._1, percorsoDueOrdinato._2, newHT,
          flag = false, matrix2, List[Int]())

        println(newHT2)
        println(moreBranch2)
        printTree(newtree2, "")
        println(matrix2.map(elem => elem._1 -> elem._2.toList).mkString("\n"))*/

  }

  @tailrec
  def calcFreqItemset(listItem: List[String], headerTable: ListMap[String, (Int, Int, List[Node[String]])], matrix: ListMap[String, Array[Int]],
                      accFreqItemset: List[(List[String], Int)])
  : List[(List[String], Int)] = {
    if (listItem.nonEmpty) {
      val head = listItem.head
      val indexToItemMap = headerTable.map(x => x._2._2 -> x._1)
      val linkedList = headerTable(head)._3
      val itemFreqItem = {
        if (matrix.contains(head))
          matrix(head).toList
        else List[Int]()
      }

      val freqItemSetSingle = fpGrowthStarCore(head, headerTable(head)._1, itemFreqItem, indexToItemMap, linkedList)
      calcFreqItemset(listItem.tail, headerTable, matrix, accFreqItemset ++ freqItemSetSingle)

    } else {
      accFreqItemset
    }
  }


  def fpGrowthStarCore(item: String, freq: Int, itemFreqItem: List[Int], indexToItemMap: ListMap[Int, String], linkedList: List[Node[String]]) = {

    if (itemFreqItem.nonEmpty) {
      val itemsStringFreq = itemFreqItem.zipWithIndex.map(x => indexToItemMap(x._2) -> x._1)
      val itemsStringFreqSorted = ListMap(itemsStringFreq.filter(_._2 >= minSupport).sortWith((elem1, elem2) => functionOrder(elem1, elem2)).zipWithIndex
        .map(x => x._1._1 -> (x._1._2, x._2)): _*)
      val matrixItem = creaMatrice(itemsStringFreqSorted)
      val headerTableItem = itemsStringFreqSorted.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))
      val treeItem = new Node[String](null, List())
      val listOfPaths = getListOfPaths(linkedList,  itemsStringFreqSorted,  List[(List[String], Int)]())
      val (newHTItem, moreBranch) = creazioneAlberoItem(treeItem, listOfPaths, headerTableItem, flag = false, matrixItem)

      if (!moreBranch) {
        if (newHTItem.nonEmpty) {
          val itemsFreq = newHTItem.map(x => x._1 -> x._2._1).toSet
          val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
          subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> freq)
        }
        else {
          List((List(item) -> freq))
        }
      } else {
        val itemsetFreq = calcFreqItemset(headerTableItem.keys.toList, headerTableItem, matrixItem, List[(List[String], Int)]())
        itemsetFreq.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> freq)
      }
    }
    else {
      List((List(item) -> freq))
    }


  }

  @tailrec
  def getListOfPaths(linkedList: List[Node[String]],  itemsStringFreqSorted: ListMap[String, (Int, Int)], accPaths: List[(List[String], Int)]): List[(List[String], Int)] = {
    if (linkedList.nonEmpty) {
      val head = linkedList.head
      val path = listaPercorsi(head, List[String]())
      val pathOrdered = (path.filter(x => itemsStringFreqSorted.contains(x)).sortBy(elem => itemsStringFreqSorted(elem)._2), head.occurrence)
      getListOfPaths(linkedList.tail,itemsStringFreqSorted, accPaths :+ pathOrdered)
    }
    else {
      accPaths
    }
  }

  @tailrec
  def creazioneAlberoItem(tree: Node[String], orderEs: List[(List[String], Int)],
                          headerTable: ListMap[String, (Int, Int, List[Node[String]])], flag: Boolean,
                          matrixItem: ListMap[String, Array[Int]]):
  (ListMap[String, (Int, Int, List[Node[String]])], Boolean) = {
    if (orderEs.nonEmpty) {
      val head = orderEs.head //Singola transazione
      val (newHeaderTable, newFlag) = addNodeTransaction2(tree, head._1, head._2, headerTable, flag, matrixItem, List[Int]()) //Ricorsivo su tutta la transazione
      creazioneAlberoItem(tree, orderEs.tail, newHeaderTable, newFlag, matrixItem) //Una volta aggiunta una transazione continuiamo con le successive
    } else (headerTable, flag) //Finite tutte le transazioni del dataset restituiamo l'ht
  }


  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthStarResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultStarSupport.txt")

}
