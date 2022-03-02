import Utils._
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable

object FPGrowthRDDOld extends App {
  val sc = getSparkContext("FPGrowthRDD")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val lines = getRDD(sc)
  val dataset = lines.map(x => x.split(spazioVirgola))

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
  def addNodeTransaction(lastNode: Node[String], transazione: Array[String], headerTable: ListMap[String, List[Node[String]]]): ListMap[String, List[Node[String]]] = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val (node, flagNewNode) = lastNode.add(transazione.head)

      //Se è stato creato lo aggiungiamo all'headerTable
      if (flagNewNode) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => List[Node[String]]() //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (transazione.head -> (old :+ node))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodeTransaction(node, transazione.tail, newTable)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodeTransaction(node, transazione.tail, headerTable)
      }
    } else {
      //Quando finisce una singola transazione
      headerTable
    }
  }

  //Creazione dell'albero
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


  //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
  def genCondTransactions(transaction: Array[String],
                          itemToRank: Map[String, Int],
                          partitioner: Partitioner): mutable.Map[Int, Array[String]] = {
    //Mappa finale da riempire
    val output = mutable.Map.empty[Int, Array[String]]
    // Ordiniamo le transazioni in base alla frequenza degli item ed eliminando gli item non frequenti
    val transFiltered = transaction.filter(item => itemToRank.contains(item)).sortBy(item => itemToRank(item))
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
      val (node, flagNewNode) = lastNode.add(path.head, countPath)

      //Se è stato creato lo aggiungiamo all'headerTable
      if (flagNewNode) {
        val old = (headerTable.get(path.head) match {
          case Some(value) => value
          case None => (List[Node[String]]()) //Non entra mai, già inizializzata dall'exec
        })

        //Aggiornamento dell'ht, si aggiorna solo la linked list dei nodi
        val newTable = headerTable + (path.head -> (old :+ node))

        //Richiamiamo questa funzione su tutti gli elementi della transazione
        addNodePath(node, path.tail, countPath, newTable)
      } else {
        //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
        addNodePath(node, path.tail, countPath, headerTable)
      }
    } else {
      //Quando abbiamo finito di scorrere tutto il path viene restituita l' ht e il flag relativo alla formazione di nuovi branch
      headerTable
    }
  }

  //Restituisce tutti gli item singoli all'interno delle liste di path
  def totalItem(listPaths: List[(List[String], Int)]): List[String] =
    listPaths.foldLeft(Set[String]())((xs, x) => xs ++ x._1).toList

  //Calcoliamo i frequentItemSet
  def createFreqItemSet(headerTable: ListMap[String, List[Node[String]]],
                        validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {
    headerTable.iterator.flatMap {
      case (item, linkedList) =>
        val itemNodeFreq = countItemNodeFreq(linkedList)
        /* Controlliamo che quel determinato item sia della partizione che stiamo considerando e quelli sotto il min supp
        * (Dopo il primo passaggio validateSuffix, sarà sempre true). */
        if (validateSuffix(item) && itemNodeFreq >= minSupport) {
          //Prendiamo tutti i percorsi per quel determinato item senza quest'ultimo
          val lPerc = linkedList.map(x => (listaPercorsi(x, List[String]()), x.occurrence))
          //Continuiamo a calcolare i conditional trees 'interni'
          val condTree = new Node[String](null, List())
          //Creiamo la nuova headerTable
          val headerTableItem = ListMap(totalItem(lPerc).map(x => x -> List[Node[String]]()): _*)
          //Creiamo l'albero condizionale dell'item e riempiamo la sua HT
          val HTItem = creazioneAlberoItem(condTree, lPerc, headerTableItem)
          //Creazione dei freqItemSet
          Iterator.single((item :: Nil, itemNodeFreq)) ++ createFreqItemSet(HTItem).map {
            case (t, c) => (item :: t, c)
          }
        } else {
          //Se non fa parte di quel gruppo restituiamo iterator vuoto
          Iterator.empty
        }
    }
  }

  def exec(): Map[Set[String], Int] = {
    //Creiamo il partitioner
    val partitioner = new HashPartitioner(numParts)

    //Prendiamo tutti gli elementi singoli con il loro count
    val firstStepVar = getSingleItemCount(partitioner)

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStepVar: _*)

    //Aggiungiamo l'indice agli item per facilitarci l'ordinamento
    val itemToRank = firstMapSorted.keys.zipWithIndex.toMap

    //Creiamo le transazioni condizionali in base al gruppo/partizione in cui si trova ogni item
    val condTrans = dataset.flatMap(transaction => genCondTransactions(transaction, itemToRank, partitioner))

    //Raggruppiamo le transazioni per gruppo/partizione e per ognuno di essi creiamo gli alberi condizionali e mappiamo con l'HT
    val condTreesHT = condTrans.groupByKey(partitioner.numPartitions).
      map(x => x._1 -> {
        val tree = new Node[String](null, List())
        creazioneAlbero(tree, x._2.toList, firstMapSorted.map(x => x._1 -> List[Node[String]]()), itemToRank).filter(_._2.nonEmpty)
      })

    //Caloliamo i frequentItemSet
    val freqItemSet = condTreesHT.flatMap(elem => createFreqItemSet(elem._2, x => partitioner.getPartition(x) == elem._1))

    freqItemSet.map(x => x._1.toSet -> x._2).collect().toMap
  }

  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthRDDResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthRDDResultSupport.txt")
}