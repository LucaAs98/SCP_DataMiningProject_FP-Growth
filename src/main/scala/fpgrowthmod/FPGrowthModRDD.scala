package fpgrowthmod

import org.apache.spark.{HashPartitioner, Partitioner}
import utils.Utils._

import scala.annotation.tailrec
import classes.{Node, Tree}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import mainClass.MainClass.minSupport

object FPGrowthModRDD extends App {
  val sc = getSparkContext("FPGrowthModRDD")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val lines = getRDD(sc)
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
                          partitioner: Partitioner): mutable.Map[Int, List[String]] = {
    //Mappa finale da riempire
    val output = mutable.Map.empty[Int, List[String]]
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

  //Calcoliamo i frequentItemSet
  def createFreqItemSet(tree: Tree, validateSuffix: String => Boolean = _ => true): Iterator[(List[String], Int)] = {
    tree.getHt.iterator.flatMap {
      case (item, (freq, linkedList)) =>
        //Calcoliamo la frequenza di un item in base ai nodi nel ht (diversa dalla frequenza presente nel ht)
        val itemNodeFreq = countItemNodeFreq(linkedList)
        /* Controlliamo che quel determinato item sia della partizione che stiamo considerando e quelli sotto il min supp
        * (Dopo il primo passaggio validateSuffix, sarà sempre true). */
        if (validateSuffix(item) && itemNodeFreq >= minSupport) {
          //Prendiamo tutti i percorsi per quel determinato item senza quest'ultimo
          val lPerc = linkedList.map(x => (listaPercorsi(x, List[String]()), x.occurrence))
          //Continuiamo a calcolare i conditional trees 'interni'
          val itemsInPaths = ListMap(totalItem(lPerc).map(x => x -> 0): _*)
          //Creazione albero condizionali e inserimento dei path in esso
          val condTree = new Tree(itemsInPaths)
          condTree.addPaths(lPerc)

          //Creazione dei freqItemSet
          Iterator.single((item :: Nil, itemNodeFreq)) ++ createFreqItemSet(condTree).map {
            case (t, c) => (item :: t, c)
          }
        } else {
          //Se non fa parte di quel gruppo restituiamo iterator vuoto
          Iterator.empty
        }
    }
  }

  /* Calcolo Frequent ItemSet per un singolo item. */
  @tailrec
  def freqItemSetOneItem(item: String, oneCondPatt: List[(List[String], Int)], accFreqItemSet: Map[Set[String], Int]): Map[Set[String], Int] = {
    if (oneCondPatt.nonEmpty) {
      //Prendiamo un percorso
      val head = oneCondPatt.head
      //Creiamo i subset con gli elementi di tale percorso + l'item preso in considerazione
      val subMap = head._1.toSet.subsets().map(elem => elem + item -> head._2).filter(_._1.nonEmpty).toMap
      //Aggiorno l'accumulatore dei frequent itemset trovati incrementando il valore delle occorrenze per tale subset
      val subMapFinal = accFreqItemSet ++ subMap.map { case (k, v) => k -> (v + accFreqItemSet.getOrElse(k, 0)) }
      //Richiamiamo questo metodo ricorsivamente su i restanti path
      freqItemSetOneItem(item, oneCondPatt.tail, subMapFinal)
    } else {
      //Restituiamo i freq itemset per un determinato item
      accFreqItemSet
    }
  }

  /* Calcolo Frequent Itemset. */
  @tailrec
  def calcFreqItemSet(cpb: ListMap[String, List[(List[String], Int)]], accFreqItemset: Map[Set[String], Int]): Map[Set[String], Int] = {
    if (cpb.nonEmpty) {
      //Prendiamo un cpb
      val oneCpb = cpb.head
      //Calcoliamo tutti i freq itemset per tale cpb
      val freqItemset = freqItemSetOneItem(oneCpb._1, oneCpb._2, Map[Set[String], Int]()).filter(item => item._2 >= minSupport)
      //Aggiungiamo i freq itemset di tale item a tutti quelli trovati in precedenza
      val newAcc = accFreqItemset ++ freqItemset
      //Continuiamo con i restanti item
      calcFreqItemSet(cpb.tail, newAcc)
    }
    else {
      //Restituiamo i freq itemset finali
      accFreqItemset
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
    //(Verificare su cloud se il partition by può darci vantaggi o meno)
    val condTrans = dataset.flatMap(transaction => genCondTransactions(transaction, itemToRank, partitioner)) //.partitionBy(partitioner)

    //Raggruppiamo le transazioni per gruppo/partizione e per ognuno di essi creiamo gli alberi condizionali e mappiamo con l'HT
    val condTrees = condTrans.groupByKey(partitioner.numPartitions).
      map(x => x._1 -> {
        //Creazione albero e inserimento transazioni del gruppo
        val tree = new Tree(firstMapSorted)
        tree.addTransactions(x._2.toList)
        tree
      })

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

    //Conditional Pattern Base per ogni gruppo
    val condPBGroup = condTrees.map(elem => elem._1 -> singleElementsCrescentOrder.map(x => x._1 -> elem._2.getAllPathsFromItem(x._1)))

    //Calcoliamo il nostro risultato finale
    val frequentItemSet = condPBGroup.flatMap(elem => calcFreqItemSet(elem._2, Map[Set[String], Int]()))

    frequentItemSet.collect().toMap
  }


  val result = time(exec())
  val numTransazioni = dataset.count().toFloat

  scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthModRDDResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "FPGrowthModRDDResultSupport.txt")
}
