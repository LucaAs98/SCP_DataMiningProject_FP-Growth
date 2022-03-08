package fpgrowthold

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import utils.Utils._
import classes.Node

object FPGrowthStarParOld extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset().par

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

  //aggiornamento delle occorrenze all'interno della matrice
  def aggiornaMatrice(matrice: ListMap[String, Array[Int]], listaPrecedenti: List[Int], item: String, count: Int): Unit = {
    val array = matrice(item)
    //Dato un item aggiorniamo le occorrenze in base alla lista degli item precedenti
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
      // Controlliamo se il nodo ha una profondità maggiore o uguale a 2, non è il primo nodo
      if (listaPrecedenti.nonEmpty) {
        aggiornaMatrice(matrice, listaPrecedenti, transazione.head, 1)
      }
      //Aggiornamento lista precedenti
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
      //Quando finisce una singola transazione restituiamo l'ht
      headerTable
    }
  }

  //Scorrendo tutte le transazioni, viene creato l'albero
  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: List[List[String]],
                      headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                      matrice: ListMap[String, Array[Int]]):
  ListMap[String, (Int, Int, List[Node[String]])] = {
    if (transactions.nonEmpty) {
      //Singola transazione
      val head = transactions.head
      //Inserimento della transazione nell'albero
      val newHeaderTable = addNodeTransaction(tree, head, headerTable, matrice, List[Int]())
      //Una volta aggiunta una transazione continuiamo con le successive
      creazioneAlbero(tree, transactions.tail, newHeaderTable, matrice)
    } else //Finite tutte le transazioni del dataset restituiamo l'ht
      headerTable
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

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int,
                  headerTable: ListMap[String, (Int, Int, List[Node[String]])], flag: Boolean,
                  matrice: ListMap[String, Array[Int]],
                  listaPrecedenti: List[Int]):
  (ListMap[String, (Int, Int, List[Node[String]])], Boolean) = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val node = lastNode.add(path.head, countPath)
      //Viene controllato se sono presenti altri branch
      val newFlag = {
        if (!flag) lastNode.sons.size > 1
        else flag
      }

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
      if (node._2) {
        val old = (headerTable.get(path.head) match {
          case Some(value) => value
          case None => (0, 0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (path.head -> (old._1, old._2, old._3 :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodePath(node._1, path.tail, countPath, newTable, newFlag, matrice, newListaPrecedenti)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodePath(node._1, path.tail, countPath, headerTable, newFlag, matrice, newListaPrecedenti)
      }
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituita l' ht e il flag relativo alla formazione di nuovi branch
      (headerTable, flag)
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

  def exec() = {

    //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList).seq.toList

    //Creiamo il nostro albero vuoto
    val newTree = new Node[String](null, List())

    // Vengono restituiti gli elementi con la loro frequenza e l'indice relativo ad essi
    //Item -> (Frequenza, Indice)
    val firstMapSortedWithIndex = firstMapSorted.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2))


    //Accumulatore per tutta l'headerTable
    val headerTable = firstMapSortedWithIndex.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))

    //Per ogni elemento associamo un array in cui vengono riportate le occorrenze relative agli altri item
    val matrix = creaMatrice(firstMapSortedWithIndex)

    //Scorriamo tutte le transazioni creando il nostro albero e restituendo l'headerTable finale
    val headerTableFinal = creazioneAlbero(newTree, orderDataset, headerTable, matrix)
    //printTree(newTree, "")

    //Vengono calcolati gli itemSet frequenti
    val allFreqitemset = calcFreqItemset(headerTableFinal.keys.toList, headerTableFinal, matrix, List[(List[String], Int)]())
    //Viene restituito il frequentItemSet come una mappa
    allFreqitemset.map(x => x._1.toSet -> x._2).toMap

  }


  //Calcolo dei frquentItemSet
  @tailrec
  def calcFreqItemset(listItem: List[String],
                      headerTable: ListMap[String, (Int, Int, List[Node[String]])],
                      matrix: ListMap[String, Array[Int]],
                      accFreqItemset: List[(List[String], Int)]): List[(List[String], Int)] = {
    if (listItem.nonEmpty) {
      //Primo item
      val head = listItem.head
      //Creazione mappa da indice -> item
      val indexToItemMap = headerTable.map(x => x._2._2 -> x._1)
      //lista dei nodi dove è presente l'item
      val linkedList = headerTable(head)._3
      //Elementi che frequentemente  sono accoppiati con l'item esaminato
      val itemFreqItem = {
        if (matrix.contains(head))
          matrix(head).toList
        else List[Int]()
      }

      //Calcolo dei ItemSet frequenti per l'item analizzato
      val freqItemSetSingle = fpGrowthStarCore(head, headerTable(head)._1, itemFreqItem, indexToItemMap, linkedList)
      calcFreqItemset(listItem.tail, headerTable, matrix, accFreqItemset ++ freqItemSetSingle)

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
      val matrixItem = creaMatrice(itemsStringFreqSorted)
      val headerTableItem = itemsStringFreqSorted.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))
      val treeItem = new Node[String](null, List())
      //Viene restituita la lista dei path relativa all'item
      val listOfPaths = getListOfPaths(linkedList, itemsStringFreqSorted, List[(List[String], Int)]())
      //Inserimento dei path nell'albero
      val (newHTItem, moreBranch) = creazioneAlberoItem(treeItem, listOfPaths, headerTableItem, flag = false, matrixItem)

      //Se l'albero ha solo un branch
      if (!moreBranch) {
        //Se l'ht non è vuota
        if (newHTItem.nonEmpty) {
          //Vengono restituiti gli elementi frequenti
          val itemsFreq = newHTItem.map(x => x._1 -> x._2._1).toSet
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
        val itemsetFreq = calcFreqItemset(newHTItem.keys.toList, newHTItem, matrixItem, List[(List[String], Int)]())
        itemsetFreq.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> freq)
      }
    }
    else {
      // Se la matrice è vuota la lista degli itemSet frequenti è composta solo dal itemSet con la sua frequenza
      List((List(item) -> freq))
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

  //Creazione Conditional FPTree per un singolo item
  @tailrec
  def creazioneAlberoItem(tree: Node[String], listPaths: List[(List[String], Int)],
                          headerTable: ListMap[String, (Int, Int, List[Node[String]])], flag: Boolean,
                          matrixItem: ListMap[String, Array[Int]]):
  (ListMap[String, (Int, Int, List[Node[String]])], Boolean) = {
    if (listPaths.nonEmpty) {
      //Viene preso il primo path
      val head = listPaths.head
      //Viene inserito il path nel Conditional FPTree
      val (newHeaderTable, newFlag) = addNodePath(tree, head._1, head._2, headerTable, flag, matrixItem, List[Int]())
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, listPaths.tail, newHeaderTable, newFlag, matrixItem)
    } else (headerTable, flag) //Esaminati tutti i path restituiamo ht e il flag dei branch
  }


  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthStarParOldResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultStarParOldSupport.txt")
}
