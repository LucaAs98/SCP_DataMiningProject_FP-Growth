import scala.annotation.tailrec
import scala.collection.immutable.ListMap

object FPGrowthPar extends App {
  //Scelta dataset (Csv e txt identitici)
  val dataset = Utils.prendiDataset("datasetKaggleAlimenti.txt")
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
  def addNodeTransaction(lastNode: Node, transazione: List[String], headerTable: ListMap[String, (Int, List[Node])]): ListMap[String, (Int, List[Node])] = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)

      //Se è stato creato lo aggiungiamo all'headerTable
      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => (0, List[Node]()) //Non entra mai, già inizializzata dall'exec
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

  def printTree(tree: Node, str: String): Unit = {
    if (tree.occurrence != -1) {
      println(str + tree.value + " " + tree.occurrence)
      tree.sons.foreach(printTree(_, str + "\t"))
    }
    else {
      tree.sons.foreach(printTree(_, str))
    }
  }

  @tailrec
  def creazioneAlbero(tree: Node, transactions: List[List[String]], headerTable: ListMap[String, (Int, List[Node])]): ListMap[String, (Int, List[Node])] = {
    if (transactions.nonEmpty) {
      val head = transactions.head //Singola transazione
      val newHeaderTable = addNodeTransaction(tree, head, headerTable) //Ricorsivo su tutta la transazione
      creazioneAlbero(tree, transactions.tail, newHeaderTable) //Una volta aggiunta una transazione continuiamo con le successive
    } else headerTable //Finite tutte le transazioni del dataset restituiamo l'ht
  }

  //Risaliamo l'albero per restituire il percorso inerente ad un nodo specifico
  @tailrec
  def listaPercorsi(nodo: Node, listaPercorsoAcc: List[String]): List[String] = {
    if (!nodo.padre.isHead) //Se non è il primo nodo
      listaPercorsi(nodo.padre, nodo.padre.value :: listaPercorsoAcc) //Continuiamo a risalire l'albero col padre
    else
      listaPercorsoAcc //Restituiamo tutto il percorso trovato
  }

  /* Prova di parallelizzazione del conditionalPatternBase, resta più veloce l'altro
      def calcoloFrequentItemset(conditionalPatternBase: (String, List[(List[String], Int)]), singleItemOccurence: ListMap[String, Int]): Map[Set[String], Int] = {

      val head = conditionalPatternBase //Prendiamo l'elemento
      val unioneListe = head._2.flatMap(_._1).distinct //Unione dei percorsi duplicati, ci ricaviamo i singoli nodi attraversati
      //Contiamo per ogni singolo elemento quante volte viene attraversato nei percorsi di head
      val countSingleItemPath = unioneListe.map(x => x -> head._2.filter(y => y._1.contains(x)).map(_._2).sum).filter(_._2 >= Utils.minSupport).toMap
      //Creiamo tutte le possibili combinazioni dei nodi attraversati per arrivare ad head (Quelli da 1 li abbiamo già)
      val subsets = countSingleItemPath.keySet.subsets().filter(_.size > 1)

      //Controlliamo all'interno dei percorsi di head che quel subset sia esistente e contiamo quante volte è presente
      val mapCountSubsets = countSingleItemPath.map(x => Set(x._1) -> x._2) ++
        subsets.toList.par.map(x => x -> head._2.filter(y => x.subsetOf(y._1.toSet))
          .map(_._2).sum).filter(_._2 >= Utils.minSupport).toMap

      //Creazione degli itemset frequenti per ogni subset trovato, ma con il count minimo tra quello di head e del subset
      val frequentItemSet = mapCountSubsets.map(x => x._1 + head._1 -> x._2.min(singleItemOccurence(head._1))) + (Set(head._1) -> (singleItemOccurence(head._1)))

      frequentItemSet
  }
  */
  @tailrec
  def calcoloFrequentItemset(conditionalPatternBase: ListMap[String, List[(List[String], Int)]], singleItemOccurence: ListMap[String, Int], accFrequentItemset: Map[Set[String], Int]): Map[Set[String], Int] = {

    if (conditionalPatternBase.nonEmpty) { //Se ci sono ancora elementi da controllare
      val head = conditionalPatternBase.head //Prendiamo l'elemento
      val unioneListe = head._2.flatMap(_._1).distinct //Unione dei percorsi duplicati, ci ricaviamo i singoli nodi attraversati
      //Contiamo per ogni singolo elemento quante volte viene attraversato nei percorsi di head
      val countSingleItemPath = unioneListe.map(x => x -> head._2.filter(y => y._1.contains(x)).map(_._2).sum).filter(_._2 >= Utils.minSupport).toMap
      //Creiamo tutte le possibili combinazioni dei nodi attraversati per arrivare ad head (Quelli da 1 li abbiamo già)
      val subsets = countSingleItemPath.keySet.subsets().filter(_.size > 1)

      //Controlliamo all'interno dei percorsi di head che quel subset sia esistente e contiamo quante volte è presente
      val mapCountSubsets = countSingleItemPath.map(x => Set(x._1) -> x._2) ++
        subsets.toList.par.map(x => x -> head._2.filter(y => x.subsetOf(y._1.toSet))
          .map(_._2).sum).filter(_._2 >= Utils.minSupport).toMap

      //Creazione degli itemset frequenti per ogni subset trovato, ma con il count minimo tra quello di head e del subset
      val frequentItemSet = mapCountSubsets.map(x => x._1 + head._1 -> x._2.min(singleItemOccurence(head._1))) + (Set(head._1) -> (singleItemOccurence(head._1)))

      //Passo ricorsivo su i successivi item
      calcoloFrequentItemset(conditionalPatternBase.tail, singleItemOccurence, accFrequentItemset ++ frequentItemSet)
    } else {
      accFrequentItemset //Finiti gli item restituiamo tutti i frequentItemSet
    }
  }

  def exec(): Map[Set[String], Int] = {
    //totalItems che rispettano il minSupport
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= Utils.minSupport) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio

    //Ordina gli item dal più frequente al meno 
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList)

    //Creiamo il nostro albero vuoto
    val newTree = new Node(null, List())

    //Accumulatore per tutta l'headerTable
    val headerTable = firstMapSorted.map(x => x._1 -> (x._2, List[Node]()))

    //Scorriamo tutte le transazioni creando il nostro albero e restituendo l'headerTable finale
    val headerTableFinal = creazioneAlbero(newTree, orderDataset, headerTable)

    //printTree(newTree, "")

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstStep.toList.sortWith((elem1, elem2) => !functionOrder(elem1, elem2)): _*)

    //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
    val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> headerTableFinal(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence)))

    //Calcoliamo il nostro risultato finale
    val frequentItemSet = calcoloFrequentItemset(conditionalPatternBase, singleElementsCrescentOrder, Map[Set[String], Int]())
    //val frequentItemSet = Utils.time(conditionalPatternBase.par.flatMap(elem => calcoloFrequentItemset(elem, singleElementsCrescentOrder)).seq)

    frequentItemSet
  }

  val result = Utils.time(exec())
  val numTransazioni = dataset.size.toFloat

  Utils.scriviSuFileFrequentItemSet(result, numTransazioni, "FPGrowthResult.txt")
  Utils.scriviSuFileSupporto(result, numTransazioni, "FPGrowthResultSupport.txt")
}