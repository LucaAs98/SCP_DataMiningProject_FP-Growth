import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.io.Source

class Node(var value: String, var sons: List[Node]) {
  var occurrence: Int = {
    if (value == null) -1 else 1
  }

  var padre: Node = null

  def isHead: Boolean = occurrence == -1

  def isEmpty: Boolean = sons.isEmpty

  def contains(newElement: String): Boolean = sons.exists(n => n.value == newElement)

  def getNodeSon(name: String): Node = {
    val node = sons.find(p => p.value == name) match {
      case Some(value) => value
      case None => null
    }
    node.occurrence += 1
    node
  }

  def getSons: List[Node] = sons

  def add(newValue: String): (Node, Boolean) = {
    if (!this.contains(newValue)) {
      val newNode = new Node(newValue, List())
      newNode.padre = this
      sons = sons :+ newNode
      (newNode, true)
    } else {
      (this.getNodeSon(newValue), false)
    }

  }

  override def toString: String = {
    "Value: " + this.value + " Occorrenze: " + this.occurrence
  }
}


object FPGrowth extends App {
  //Scelta dataset (Csv e txt identitici)
  val file = "src/main/resources/dataset/datasetGrande.txt"
  val dataset = datasetFromFile(file)
  val transaction = List(Set("a", "c", "d", "f", "g", "i", "m", "p")
    , Set("a", "b", "c", "f", "i", "m", "o")
    , Set("b", "f", "h", "j", "o")
    , Set("b", "c", "k", "s", "p")
    , Set("a", "c", "e", "f", "l", "m", "n", "p"))
  val min_sup = 30

  def datasetFromFile(nfile: String): List[Set[String]] = {
    //Creazione file
    val source = Source.fromFile(nfile)
    //Vengono prese le linee del file e separate, creando una list di set di stringhe
    val data = source.getLines().map(x => x.split(" ").toSet).toList
    data.foreach(println)
    source.close()
    data
  }

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[String]): Map[String, Int] = {
    (item map (x => x -> (dataset count (y => y.contains(x))))).toMap
  }

  val totalItem = (dataset reduce ((xs, x) => xs ++ x)).toList

  def datasetFilter(firstStep: List[String]) = {
    dataset.map(x => x.toList.filter(elemento => firstStep.contains(elemento)).
      sortBy(firstStep.indexOf(_)))
  }

  def functionOrder(y: (String, Int), z: (String, Int)): Boolean = {
    if (y._2 == z._2)
      y._1 < z._1
    else
      y._2 > z._2
  }

  @tailrec
  def addNodeTransaction(lastNode: Node, transazione: List[String], headerTable: ListMap[String, (Int, List[Node])]): ListMap[String, (Int, List[Node])] = {

    if (transazione.nonEmpty) {
      val node = lastNode.add(transazione.head)

      if (node._2) {
        val old = (headerTable.get(transazione.head) match {
          case Some(value) => value
          case None => (0, List[Node]())
        })

        val newTable = headerTable + (transazione.head -> (old._1, old._2 :+ node._1))

        addNodeTransaction(node._1, transazione.tail, newTable)
      } else {
        addNodeTransaction(node._1, transazione.tail, headerTable)
      }
    }
    else {
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
  def scorrimento(tree: Node, transactions: List[List[String]], headerTable: ListMap[String, (Int, List[Node])]): ListMap[String, (Int, List[Node])] = {
    if (transactions.nonEmpty) {
      val head = transactions.head
      val newHeaderTable = addNodeTransaction(tree, head, headerTable)
      scorrimento(tree, transactions.tail, newHeaderTable)
    }
    else
      headerTable
  }

  @tailrec
  def listaPercorsi(nodo: Node, listaPercorsoAcc: List[String]): List[String] = {
    if (!nodo.padre.isHead)
      listaPercorsi(nodo.padre, nodo.padre.value :: listaPercorsoAcc)
    else
      listaPercorsoAcc
  }

  @tailrec
  def calcoloFrequentItemset(conditionalPatternBase: ListMap[String, List[(List[String], Int)]], singleItemOccurence: ListMap[String, Int], accFrequentItemset: Map[Set[String], Int]): Map[Set[String], Int] = {

    if (conditionalPatternBase.nonEmpty) {
      val head = conditionalPatternBase.head
      val unioneListe = head._2.flatMap(_._1).distinct
      val countSingleItemPath = unioneListe.map(x => x -> head._2.filter(y => y._1.contains(x)).map(y => y._2).sum).toMap.filter(_._2 >= min_sup)
      val subsets = countSingleItemPath.keySet.subsets().filter(_.size > 1)
      val mapCountSubsets = countSingleItemPath.map(x => Set(x._1) -> x._2) ++ subsets.map(x => x -> head._2.filter(y => x.subsetOf(y._1.toSet))
        .map(y => y._2).sum).toMap.filter(_._2 >= min_sup)
      val frequentItemSet = mapCountSubsets.map(x => x._1 + head._1 -> x._2.min(singleItemOccurence(head._1))) + (Set(head._1) -> (singleItemOccurence(head._1)))

      calcoloFrequentItemset(conditionalPatternBase.tail, singleItemOccurence, accFrequentItemset ++ frequentItemSet)

    } else {
      accFrequentItemset
    }

  }

  def exec(): Unit = {

    val firstStep = countItemSet(totalItem).filter(x => x._2 >= min_sup) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    //firstStep.toList.sortBy(_._2).foreach(println(_))
    val firstMapSorted = ListMap(firstStep.toList.sortWith((y, z) => functionOrder(y, z)): _*)
    //firstMapSorted.foreach(println)
    val headerTable = firstMapSorted.map(x => x._1 -> (x._2, List[Node]()))

    val orderDataset = datasetFilter(firstMapSorted.keys.toList)
    //orderDataset.foreach(println(_))
    val newTree = new Node(null, List())

    val headerTableFinal = scorrimento(newTree, orderDataset, headerTable)
    //headerTableFinal.foreach(println)
    printTree(newTree, "")

    val singleElementsCrescentOrder = ListMap(firstStep.toList.sortWith((y, z) => !functionOrder(y, z)): _*)

    //singleElementsCrescentOrder.foreach(println)

    val conditionalPatternBase2 = singleElementsCrescentOrder.map(x => x._1 -> headerTableFinal(x._1)._2.map(y => (listaPercorsi(y, List[String]()), y.occurrence)))
    //conditionalPatternBase2.foreach(println)
    /*
    println("Tempo presa della testa")
    val head = time(conditionalPatternBase2.head)
    println("Singoli item nelle liste")
    val unioneListe = time(head._2.flatMap( _._1 ).distinct)
    println("Conto degli elemnti singoli nelle liste")
    val countSingleItemPath = time(unioneListe.map(x => x -> head._2.filter(y => y._1.contains(x)).map(y => y._2).sum).toMap.filter(_._2 >= min_sup))
    println("Calcolo subset")
    val subsets = time(countSingleItemPath.keySet.subsets().filter(_.size > 1))
    println("Conteggio negli elementi dei sottoinsime")
    val mapCountSubsets = time(countSingleItemPath.map(x => Set(x._1) -> x._2) ++ subsets.map(x => x -> head._2
      .foldLeft(0)((op1,op2) => op1 + {if(x.subsetOf(op2._1.toSet)) op2._2 else 0})).toMap.filter(_._2 >= min_sup))
    println("\nfrequent Itemset:")
    val frequentItemSet = time(mapCountSubsets.map(x => x._1 + head._1 -> x._2.min(singleElementsCrescentOrder(head._1))) + (Set(head._1) -> (singleElementsCrescentOrder(head._1))))
    */

    val frequentItemSet = calcoloFrequentItemset(conditionalPatternBase2, singleElementsCrescentOrder, Map[Set[String], Int]())
    //frequentItemSet.toList.sortBy(_._1.size).foreach(println)

  }

  def time[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  time(exec())

}
