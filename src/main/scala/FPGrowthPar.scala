import Utils._

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.parallel.ParMap

object FPGrowthPar extends App {
  val dataset = Utils.prendiDataset("T10I4D100K.txt").par
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

  //Aggiungiamo i nodo di una transazione all'albero
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
      //Quando finisce una singola transazione restituiamo l'ht
      headerTable
    }
  }

  //Stampa l'albero
  def printTree(tree: Node[String], str: String): Unit = {
    if (tree.occurrence != -1) {
      println(str + tree.value + " " + tree.occurrence)
      tree.sons.foreach(printTree(_, str + "\t"))
    }
    else {
      tree.sons.foreach(printTree(_, str))
    }
  }

  //Scorrendo tutte le transazioni, viene creato l'albero
  @tailrec
  def creazioneAlbero(tree: Node[String], transactions: List[List[String]], headerTable: ListMap[String, (Int, List[Node[String]])]): ListMap[String, (Int, List[Node[String]])] = {
    if (transactions.nonEmpty) {
      //Singola transazione
      val head = transactions.head
      //Inserimento della transazione nell'albero
      val newHeaderTable = addNodeTransaction(tree, head, headerTable)
      //Una volta aggiunta una transazione continuiamo con le successive
      creazioneAlbero(tree, transactions.tail, newHeaderTable)
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

  //Aggiungiamo i nodo di un path all'albero
  @tailrec
  def addNodePath(lastNode: Node[String], path: List[String], countPath: Int,
                  headerTable: ListMap[String, (Int, List[Node[String]])], flag: Boolean):
  (ListMap[String, (Int, List[Node[String]])], Boolean) = {

    if (path.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val node = lastNode.add(path.head, countPath)
      //Viene controllato se sono presenti altri branch
      val newFlag = {
        if (!flag) lastNode.sons.size > 1
        else flag
      }

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(path.head) match {
          case Some(value) => value
          case None => (0, List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (path.head -> (old._1, old._2 :+ node._1))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodePath(node._1, path.tail, countPath, newTable, newFlag)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodePath(node._1, path.tail, countPath, headerTable, newFlag)
      }
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituita l' ht e il flag relativo alla formazione di nuovi branch
      (headerTable, flag)
    }
  }

  //Creazione Conditional FPTree per un singolo item
  @tailrec
  def creazioneAlberoItem(tree: Node[String], sortedPaths: List[(List[String], Int)],
                          headerTable: ListMap[String, (Int, List[Node[String]])], flag: Boolean):
  (ListMap[String, (Int, List[Node[String]])], Boolean) = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      val (newHeaderTable, newFlag) = addNodePath(tree, head._1, head._2, headerTable, flag)
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      creazioneAlberoItem(tree, sortedPaths.tail, newHeaderTable, newFlag)
    } else (headerTable, flag) //Esaminati tutti i path restituiamo ht e il flag dei branch
  }

  //Ordiniamo i path per le occorrenze, eliminando gli item sotto il minimo supporto
  def condPBSingSort(listOfPaths: List[(List[String], Int)], elementSorted: List[String]) = {
    listOfPaths.map(elem => elem._1.filter(elementSorted.contains).sortBy(elementSorted.indexOf(_)) -> elem._2)
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

  //Per ogni singolo elemento viene costituito il conditional FPTree per poi calcolare gli elementi frequenti
  def freqItemsetCondPB(item: String, headList: List[(List[String], Int)], firstMapSorted: ListMap[String, Int]): List[(List[String], Int)] = {
    //Conteggio elementi relativi Conditional Pattern Base
    val itemCount = countItemConPB(headList, Map.empty[String, Int]).filter(x => x._2 >= minSupport)
    //Vengono restituiti tutti gli elementi oridinati alfabeticamente e in ordine decrescente per numero di occorrenze
    val itemMapSorted = ListMap(itemCount.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)
    //Otteniamo i path ordinati per le occorrenze
    val orderedPath = condPBSingSort(headList, itemMapSorted.keys.toList)
    //creazione ht e del Conditional FPTree
    val headerTableItem = itemMapSorted.map(x => x._1 -> (x._2, List[Node[String]]()))
    val condTreeItem = new Node[String](null, List())
    val (headerTableItemFin, moreBranch) = creazioneAlberoItem(condTreeItem, orderedPath, headerTableItem, flag = false)

    //Se l'albero creato ha un signolo branch
    if (!moreBranch) {
      //Se l'ht non è vuota
      if (headerTableItemFin.nonEmpty) {
        //Vengono restituiti gli elementi frequenti
        val itemsFreq = headerTableItemFin.map(x => x._1 -> x._2._1).toSet
        //Vegono create tutte le possibili combinazioni tra gli item frequenti
        val subItemsFreq = itemsFreq.subsets().filter(_.nonEmpty).toList
        //Vengono ottenuti i frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
        subItemsFreq.map(set => calcoloMinimi(set)).map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item))
      }
      else {
        // se ht è vuota la lista degli itemSet frequenti è composta solo dal itemSet con la sua frequenza
        List((List(item) -> firstMapSorted(item)))
      }
    } else {
      //Viene ricostruito il conditionalPB per gli item frequenti relativi al item di partenza
      val itemCresOrder = ListMap(itemMapSorted.toList.reverse: _*)
      val newCondPB = itemCresOrder.map(x => x._1 -> headerTableItemFin(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence))).par
      // calcolo frequentItemSet (aggiungendo a questi l'item) per poi aggiungere l'item di partenza alla lista degli item più frequenti
      val freqItemsetItem = condFPTree(newCondPB, itemMapSorted)
      freqItemsetItem.map(x => (x._1 :+ item) -> x._2) :+ (List(item) -> firstMapSorted(item))
    }
  }


  def condFPTree(conditionalPatternBase: ParMap[String, List[(List[String], Int)]],
                 firstMapSorted: ListMap[String, Int]): List[(List[String], Int)] = {

    val freqItemsets = conditionalPatternBase.map(elem => freqItemsetCondPB(elem._1, elem._2, firstMapSorted))
    freqItemsets.flatten.toList
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

    //Accumulatore per tutta l'headerTable iniziale
    val headerTable = firstMapSorted.map(x => x._1 -> (x._2, List[Node[String]]()))

    //Scorriamo tutte le transazioni creando il nostro albero e restituendo l'headerTable finale
    val headerTableFinal = creazioneAlbero(newTree, orderDataset, headerTable)

    //printTree(newTree, "")

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

    //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
    val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> headerTableFinal(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence))).par

    //Vengono calcolati gli itemSet frequenti
    val allFreqitemset = condFPTree(conditionalPatternBase, firstMapSorted)
    //Viene restituito il frequentItemSet come una mappa
    allFreqitemset.map(x => x._1.toSet -> x._2).toMap
  }

  val result = time(exec())
  val numTransazioni = dataset.size.toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthParResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultParSupport.txt")
}
