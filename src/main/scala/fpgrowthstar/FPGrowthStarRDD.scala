package fpgrowthstar

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import utils.Utils._
import classes.{TreeStar, Node}

object FPGrowthStarRDD {
  //Esecuzione effettiva dell'algoritmo
  def exec(minSupport: Int, numParts: Int, pathInput:String): (Map[Set[String], Int], Long, Float) = {

    val sc = getSparkContext("FPGrowthStarRDDOld")
    //Prendiamo il dataset (vedi Utils per dettagli)
    val (lines, dimDataset) = getRDD(pathInput,sc)
    val dataset = lines.map(x => x.split(" "))

    //Contiamo, filtriamo e sortiamo tutti gli elementi nel dataset
    def getSingleItemCount(partitioner: Partitioner): Array[(String, Int)] = {
      dataset.flatMap(t => t).map(v => (v, 1))
        .reduceByKey(partitioner, _ + _)
        .filter(_._2 >= minSupport)
        .collect()
        .sortBy(x => (-x._2, x._1))
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
                            partitioner: Partitioner): mutable.Map[Int, List[String]] = {
      //Mappa finale da riempire
      val output = mutable.Map.empty[Int, List[String]]
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
          output(part) = transFiltered.slice(0, i + 1).toList
        }
        i -= 1
      }
      output
    }

    //Occorrenze totali di quell'item
    def countItemNodeFreq(list: List[Node[String]]): Int = {
      list.foldLeft(0)((x, y) => x + y.occurrence)
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
        //Restituzione della lista dei path
        accPaths
      }
    }

    //Calcoliamo i frequentItemSet
    def createFreqItemSet(tree: TreeStar,
                          validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {
      //Creazione mappa da indice -> item
      val headerTable = tree.getHt
      val indexToItemMap = headerTable.map(x => x._2._2 -> x._1)
      headerTable.iterator.flatMap {
        case (item, (itemNodeFreq, index, linkedList)) =>
          /* Controlliamo che quel determinato item sia della partizione che stiamo considerando e quelli sotto il min supp
          * (Dopo il primo passaggio validateSuffix, sarà sempre true). */
          if (validateSuffix(item) && itemNodeFreq >= minSupport) {

            //Elementi che frequentemente  sono accoppiati con l'item esaminato
            val itemFreqItem = tree.getFreqItems(item)

            if (itemFreqItem.nonEmpty) {
              //Ricaviamo l'item sottoforma di stringa dagli indici degli array, ordinati per occorrenze
              val itemsStringFreq = ListMap(itemFreqItem.zipWithIndex.map(x => indexToItemMap(x._2) -> (x._1, x._2)).filter(_._2._1 > 0): _*)
              val lPerc = linkedList.map(x => (listaPercorsi(x, List[String]()), x.occurrence))
              val treeItem = new TreeStar(itemsStringFreq)
              treeItem.addPaths(lPerc)

              //Creazione dei freqItemSet
              Iterator.single((item :: Nil, itemNodeFreq)) ++ createFreqItemSet(treeItem).map {
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


    def avviaAlgoritmo(): Map[Set[String], Int] = {
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
          val treeGroup = new TreeStar(firstMapSortedWithIndex)
          treeGroup.addTransactions(x._2.toList)
          treeGroup
        })

      //Caloliamo i frequentItemSet
      val freqItemSet = condHTMatrix.flatMap(elem => createFreqItemSet(elem._2, x => partitioner.getPartition(x) == elem._1))

      freqItemSet.map(x => x._1.toSet -> x._2).collect().toMap
    }

    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }
}
