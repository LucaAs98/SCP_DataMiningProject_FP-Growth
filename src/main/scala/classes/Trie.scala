package classes

import scala.annotation.tailrec
import scala.collection.mutable

class Trie(var itemMap: Map[String, (Int, Int)]) extends Serializable {
  //Inizializziamo il Trie partendo dal solito firstMapWithIndex
  val root = new NodeTrie(null, List())
  var moreBranch: Boolean = false
  //Numero nodi diversi per ciascun item
  var counterDifferentNode: mutable.Map[Int, Int] = mutable.Map(itemMap.map(_._2._1 -> 0).toSeq: _*)
  //Numero nodi dell'intero trie
  var nodeCounter = 0


  def getItemMap: Map[String, (Int, Int)] = {
    itemMap
  }

  def nonEmptyTrie: Boolean = {
    root.sons.nonEmpty
  }

  //Aggiungiamo le transazioni al trie
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

  //Aggiungiamo i nodo di una transazione al trie
  @tailrec
  private def addNodeTransaction(lastNode: NodeTrie, transazione: List[String]): Unit = {
    //Passo base
    if (transazione.nonEmpty) {
      //Prendiamo un singolo elemento della transazione
      val item = transazione.head

      //Aggiungiamo all'ultimo nodo creato il nuovo
      val (node, flagNewNode) = lastNode.add(item)

      //Se abbiamo aggiunto un nuovo nodo aggiorniamo il counter totale dei nodi del trie e quello del singolo item
      if (flagNewNode) {
        nodeCounter = nodeCounter + 1
        counterDifferentNode(itemMap(node.value)._1) += 1
      }
      //Iteriamo sul resto della transazione
      addNodeTransaction(node, transazione.tail)
    }
  }

  //Aggiungiamo i path al nostro trie condizionale
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

  //Aggiungiamo i nodo di un path al trie
  @tailrec
  private def addNodePath(lastNode: NodeTrie, path: List[String], countPath: Int): Unit = {

    if (path.nonEmpty) {
      val item = path.head
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val (node, flagNewNode) = lastNode.add(item, countPath)

      //Viene controllato se sono presenti altri branch
      if (!moreBranch && lastNode.sons.size > 1) moreBranch = true

      //Se abbiamo aggiunto un nuovo nodo aggiorniamo il counter totale dei nodi del trie e quello del singolo item
      if (flagNewNode) {
        counterDifferentNode(itemMap(node.value)._1) += 1
        nodeCounter += 1
      }

      //Iteriamo sul resto del path
      addNodePath(node, path.tail, countPath)
    }
  }

  def printTree(): Unit = {

    //Metodo per la stampa dell'albero
    def printTreeRec(node: NodeTrie, str: String): Unit = {
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

  def merge(other: Trie): Trie = {

    def mergeVertical(toAdd: NodeTrie, last: NodeTrie): Unit = {

      val item = toAdd.value

      val (newLastNode, flagNewNode) = last.add(item, toAdd.occurrence)

      mergeHorizontal(toAdd.sons, newLastNode)
    }

    @tailrec
    def mergeHorizontal(listSons: List[NodeTrie], last: NodeTrie): Unit = {
      if (listSons.nonEmpty) {
        val head = listSons.head
        mergeVertical(head, last)
        mergeHorizontal(listSons.tail, last)
      }
    }

    mergeHorizontal(other.root.sons, this.root)
    this
  }

  //RDD: Dobbiamo pulire l'item map togliendo gli item non presenti e riassegnando gli indici agli item in base alla frequenza
  def clearitToMapAndCDifNod(): Unit = {
    itemMap = itemMap.filter(x => counterDifferentNode(x._2._1) > 0).toList
      .sortBy(_._2._1).zipWithIndex.map(elem => elem._1._1 -> (elem._2, elem._1._2._2)).toMap

    counterDifferentNode = mutable.Map(counterDifferentNode.filter(_._2 > 0).toList
      .sortBy(_._1).zipWithIndex.map(elem => elem._2 -> elem._1._2): _*)
  }
}