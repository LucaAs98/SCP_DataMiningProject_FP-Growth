package classes

import scala.annotation.tailrec
import scala.collection.immutable.ListMap


class Tree(firstMapSorted: ListMap[String, Int]) extends Serializable{

  //Inizializzazione radice
  val root = new Node[String](null, List())
  //Inizializzazione ht
  var headerTable: ListMap[String, (Int, List[Node[String]])] = firstMapSorted.map(x => x._1 -> (x._2, List[Node[String]]()))
  var moreBranch: Boolean = false

  //Aggiunta transazioni all'albero
  @tailrec
  final def addTransactions(transactions: List[List[String]]): Unit = {
    if (transactions.nonEmpty) {
      //Singola transazione
      val head = transactions.head
      //Inserimento della transazione nell'albero
      addNodeTransaction(root, head)
      //Una volta aggiunta una transazione continuiamo con le successive
      this.addTransactions(transactions.tail)
    }
  }

  //Aggiungiamo i nodo di una transazione all'albero
  @tailrec
  private def addNodeTransaction(lastNode: Node[String], transazione: List[String]): Unit = {
    //Passo base
    if (transazione.nonEmpty) {

      //Viene preso il primo item della transazione
      val item = transazione.head

      //Aggiungiamo all'ultimo nodo creato il nuovo
      val (node, flagNewNode) = lastNode.add(item)

      //Se viene creato un nuovo nodo aggiorniamo l'ht
      if (flagNewNode) {
        val valueItem = headerTable(item)
        headerTable = headerTable + (item -> (valueItem._1, valueItem._2 :+ node))
      }

      //Si passa al nodo successivo
      addNodeTransaction(node, transazione.tail)
    }
  }

  @tailrec
  final def addPaths(sortedPaths: List[(List[String], Int)]): Unit = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      addNodePath(root, head._1, head._2)
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      addPaths(sortedPaths.tail)
    }
  }

  //Aggiungiamo il nodo di un path all'albero
  @tailrec
  private def addNodePath(lastNode: Node[String], path: List[String], countPath: Int): Unit = {

    if (path.nonEmpty) {

      //Viene preso il primo item della transazione
      val item = path.head

      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val (node, flagNewNode) = lastNode.add(item, countPath)

      //Viene controllato se sono presenti altri branch
      if (!moreBranch && lastNode.sons.size > 1) moreBranch = true

      //Se viene creato un nuovo nodo aggiorniamo l'ht
      if (flagNewNode) {
        val valueItem = headerTable(item)
        headerTable = headerTable + (item -> (valueItem._1, valueItem._2 :+ node))
      }

      //Si passa al nodo successivo
      addNodePath(node, path.tail, countPath)
    }
  }

  //Risaliamo l'albero per restituire il percorso inerente ad un nodo specifico
  @tailrec
  private def getPathNode(nodo: Node[String], listaPercorsoAcc: List[String]): List[String] = {
    if (!nodo.padre.isHead) //Se non è il primo nodo
      getPathNode(nodo.padre, nodo.padre.value :: listaPercorsoAcc) //Continuiamo a risalire l'albero col padre
    else
      listaPercorsoAcc //Restituiamo tutto il percorso trovato
  }

  //Dato un item restituisce tutti i path relativi a esso
  def getAllPathsFromItem(item: String): List[(List[String], Int)] = {
    headerTable(item)._2.map(node => (getPathNode(node, List[String]()), node.occurrence))
  }

  def getHt: Map[String, (Int, List[Node[String]])] = {
    headerTable
  }

  def getIfNonEmptyHt: Boolean = {
    headerTable.nonEmpty
  }

  def printTree(): Unit = {

    //Metodo per la stampa dell'albero
    def printTreeRec(node: Node[String], str: String): Unit = {
      if (node.occurrence != -1) {
        println(str + node.value + " " + node.occurrence)
        node.sons.foreach(printTreeRec(_, str + "\t"))
      }
      else {
        node.sons.foreach(printTreeRec(_, str))
      }
    }
    printTreeRec(root, "")
  }

  def merge(other: Tree): Tree = {

    def mergeVertical(toAdd: Node[String], last: Node[String]): Unit = {

      val item = toAdd.value

      val (newLastNode, flagNewNode) = last.add(item, toAdd.occurrence)

      if (flagNewNode) {
        val valueItem = headerTable(item)
        headerTable = headerTable + (item -> (valueItem._1, valueItem._2 :+ newLastNode))
      }

      mergeHorizontal(toAdd.sons, newLastNode)
    }

    @tailrec
    def mergeHorizontal(listSons: List[Node[String]], last: Node[String]): Unit = {
      if (listSons.nonEmpty) {
        val head = listSons.head
        mergeVertical(head, last)
        mergeHorizontal(listSons.tail, last)
      }
    }

    mergeHorizontal(other.root.sons, this.root)
    this
  }


}
