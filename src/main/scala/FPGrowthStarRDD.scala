import Utils._
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable

object FPGrowthStarRDD extends App {
  val sc = getSparkContext("FPGrowthStarRDD")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val lines = getRDD(sc)
  val dataset = lines.map(x => x.split(spazioVirgola))

  val numParts = 100

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

  //Aggiornamento delle occorrenze all'interno della matrice
  def aggiornaMatrice(matrice: ListMap[String, Array[Int]], listaPrecedenti: List[Int], item: String, count: Int): Unit = {
    val array = matrice(item)
    //Dato un item aggiorniamo le occorrenze in base alla lista degli item precedenti
    listaPrecedenti.foreach(index => array(index) += count)
  }

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction(lastNode: Node[String], transazione: Array[String],
                         headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                         matrice: ListMap[String, Array[Int]],
                         listaPrecedenti: List[Int]):
  ListMap[String, (Int, Int, List[Node[String]])] = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val (node, flagNewNode) = lastNode.add(transazione.head)

      // Controlliamo se il nodo ha una profondità maggiore o uguale a 2, non è il primo nodo
      if (listaPrecedenti.nonEmpty) {
        aggiornaMatrice(matrice, listaPrecedenti, transazione.head, 1)
      }
      //Aggiornamento lista precedenti
      val newListaPrecedenti = listaPrecedenti :+ headerTable(transazione.head)._2

      val old = (headerTable.get(transazione.head) match {
        case Some(value) => value
        case None => (0, 0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
      })

      //Se è stato creato lo aggiungiamo all'headerTable
      val newTable = if (flagNewNode) {
        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        headerTable + (transazione.head -> (old._1 + 1, old._2, old._3 :+ node))
      } else {
        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        headerTable + (transazione.head -> (old._1 + 1, old._2, old._3))
      }

      //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
      addNodeTransaction(node, transazione.tail, newTable, matrice, newListaPrecedenti)

    } else {
      //Quando finisce una singola transazione restituiamo l'ht
      headerTable
    }
  }

  //Creazione dell'albero
  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: List[Array[String]],
                      headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                      matrice: ListMap[String, Array[Int]]): ListMap[String, (Int, Int, List[Node[String]])] = {
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


  //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
  def genCondTransactions(transaction: Array[String],
                          firstMapSortedWithIndex: Map[String, (Int, Int)],
                          partitioner: Partitioner): mutable.Map[Int, Array[String]] = {
    //Mappa finale da riempire
    val output = mutable.Map.empty[Int, Array[String]]
    // Ordiniamo le transazioni in base alla frequenza degli item ed eliminando gli item non frequenti
    val transFiltered = transaction.filter(item => firstMapSortedWithIndex.contains(item)).sortBy(item => firstMapSortedWithIndex(item)._2)
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

  //Occorrenze totali di quell'item
  def countItemNodeFreq(list: List[Node[String]]): Int = {
    list.foldLeft(0)((x, y) => x + y.occurrence)
  }

  //Creazione Conditional FPTree per un singolo item
  @tailrec
  def creazioneAlberoItem(tree: Node[String], sortedPaths: List[(List[String], Int)],
                          headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                          matrixItem: ListMap[String, Array[Int]]):
  ListMap[String, (Int, Int, List[Node[String]])] = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      val newHeaderTable = addNodePath(tree, head._1, head._2, headerTable, matrixItem, List[Int]())
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, sortedPaths.tail, newHeaderTable, matrixItem)
    } else headerTable //Esaminati tutti i path restituiamo ht e il flag dei branch
  }

  //Aggiungiamo i nodo di un path all'albero
  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int,
                  headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                  matrice: ListMap[String, Array[Int]],
                  listaPrecedenti: List[Int]):
  ListMap[String, (Int, Int, List[Node[String]])] = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val (node, flagNewNode) = lastNode.add(path.head, countPath)
      // Viene aggiornata la lista dei precedenti
      val newListaPrecedenti = {
        if (matrice.nonEmpty) {
          if (listaPrecedenti.nonEmpty) {
            aggiornaMatrice(matrice, listaPrecedenti, path.head, countPath)
          }
          listaPrecedenti :+ headerTable(path.head)._2
        } else {
          listaPrecedenti
        }
      }
      //Se è stato creato lo aggiungiamo all'headerTable
      if (flagNewNode) {
        val old = (headerTable.get(path.head) match {
          case Some(value) => value
          case None => (0, 0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (path.head -> (old._1, old._2, old._3 :+ node))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodePath(node, path.tail, countPath, newTable, matrice, newListaPrecedenti)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodePath(node, path.tail, countPath, headerTable, matrice, newListaPrecedenti)
      }
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituita l' ht e il flag relativo alla formazione di nuovi branch
      headerTable
    }
  }

  //Restituisce tutti gli item singoli all'interno delle liste di path
  def totalItem(listPaths: List[(List[String], Int)]): List[String] =
    listPaths.foldLeft(Set[String]())((xs, x) => xs ++ x._1).toList

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

  //Calcoliamo i frequentItemSet
  def createFreqItemSet(headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                        matrix: ListMap[String, Array[Int]],
                        validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {
    //Creazione mappa da indice -> item
    val indexToItemMap = headerTable.map(x => x._2._2 -> x._1)
    headerTable.iterator.flatMap {
      case (item, (itemNodeFreq, index, linkedList)) =>
        /* Controlliamo che quel determinato item sia della partizione che stiamo considerando e quelli sotto il min supp
        * (Dopo il primo passaggio validateSuffix, sarà sempre true). */
        if (validateSuffix(item) && itemNodeFreq >= minSupport) {

          //Elementi che frequentemente  sono accoppiati con l'item esaminato
          val itemFreqItem = {
            if (matrix.contains(item))
              matrix(item).toList
            else List[Int]()
          }

          if (itemFreqItem.nonEmpty) {
            //Ricaviamo l'item sottoforma di stringa dagli indici degli array, ordinati per occorrenze
            val itemsStringFreq = ListMap(itemFreqItem.zipWithIndex.map(x => indexToItemMap(x._2) -> (x._1, x._2)).filter(_._2._1 > 0): _*)

            //Creazione della matrice, ht e dell'albero
            val matrixItem = creaMatrice(itemsStringFreq)
            val headerTableItem = itemsStringFreq.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))

            //Creiamo l'albero condizionale dell'item e riempiamo la sua HT
            val treeItem = new Node[String](null, List())
            //Viene restituita la lista dei path relativa all'item
            val lPerc = linkedList.map(x => (listaPercorsi(x, List[String]()), x.occurrence))

            val HTItem = creazioneAlberoItem(treeItem, lPerc, headerTableItem, matrixItem)
            //Creazione dei freqItemSet
            Iterator.single((item :: Nil, itemNodeFreq)) ++ createFreqItemSet(HTItem, matrixItem).map {
              case (t, c) => (item :: t, c)
            }
          } else {
            //Creazione dei freqItemSet singolo
            Iterator.single((item :: Nil, itemNodeFreq))
          }

        } else {
          //Se non fa parte di quel gruppo restituiamo iterator vuoto
          Iterator.empty
        }
    }
  }

  //Per ogni elemento associamo un array in cui vengono riportate le occorrenze relative agli altri item
  def creaMatrice(listSorted: ListMap[String, (Int, Int)]) = {
    if (listSorted.size > 1) {
      listSorted.tail.map(elem => elem._1 -> new Array[Int](elem._2._2).map(x => 0))
    }
    else
      ListMap.empty[String, Array[Int]]
  }

  def exec(): Map[Set[String], Int] = {
    //Creiamo il partitioner
    val partitioner = new HashPartitioner(numParts)

    //Prendiamo tutti gli elementi singoli con il loro count
    val firstStepVar = getSingleItemCount(partitioner)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStepVar: _*)

    // Vengono restituiti gli elementi con la loro frequenza e l'indice relativo ad essi
    //Item -> (Frequenza, Indice)
    val firstMapSortedWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2))

    //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
    val condTrans = dataset.flatMap(transaction => genCondTransactions(transaction, firstMapSortedWithIndex, partitioner))

    //Raggruppiamo le transazioni per gruppo/partizione e per ognuno di essi creiamo gli alberi condizionali e mappiamo con l'HT
    val condHTMatrix = condTrans.groupByKey(partitioner.numPartitions).
      map(x => x._1 -> {
        val treeGroup = new Node[String](null, List())
        //Accumulatore per tutta l'headerTable
        val headerTableGroup = firstMapSortedWithIndex.map(x => x._1 -> (0, x._2._2, List[Node[String]]()))
        //Per ogni elemento associamo un array in cui vengono riportate le occorrenze relative agli altri item
        val matrixGroup = creaMatrice(firstMapSortedWithIndex)
        val headerTableGroupFinal = creazioneAlbero(treeGroup, x._2.toList, headerTableGroup, matrixGroup)
        (headerTableGroupFinal, matrixGroup)
      })

    //Caloliamo i frequentItemSet
    val freqItemSet = condHTMatrix.flatMap(elem => createFreqItemSet(elem._2._1, elem._2._2, x => partitioner.getPartition(x) == elem._1))

    freqItemSet.map(x => x._1.toSet -> x._2).collect().toMap
  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthStarRDDResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthStarRDDResultSupport.txt")
}