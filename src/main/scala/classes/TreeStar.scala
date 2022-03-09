package classes

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

class TreeStar(firstMapSortedWithIndex: ListMap[String, (Int, Int)]) extends Serializable {
  //Inizializzazione albero
  val root = new Node[String](null, List())
  var headerTable: ListMap[String, (Int, Int, List[Node[String]])] = firstMapSortedWithIndex.map(x => x._1 -> (x._2._1, x._2._2, List[Node[String]]()))
  var moreBranch: Boolean = false
  val matrix: ListMap[String, Array[Int]] = creaMatrice(firstMapSortedWithIndex)

  //Creiamo la matrice dati gli item ordinati con le frequenze
  def creaMatrice(listSorted: ListMap[String, (Int, Int)]): ListMap[String, Array[Int]] = {
    if (listSorted.size > 1) {
      listSorted.tail.map(elem => elem._1 -> new Array[Int](elem._2._2).map(x => 0))
    }
    else
      ListMap.empty[String, Array[Int]]
  }

  //Aggiunge una transazione all'albero
  @tailrec
  final def addTransactions(transactions: List[List[String]]): Unit = {
    if (transactions.nonEmpty) {
      //Singola transazione
      val head = transactions.head
      //Inserimento della transazione nell'albero
      addNodeTransaction(root, head, List[Int]())
      //Una volta aggiunta una transazione continuiamo con le successive
      this.addTransactions(transactions.tail)
    }
  }

  //Aggiungiamo i nodo di una transazione all'albero
  @tailrec
  private def addNodeTransaction(lastNode: Node[String], transazione: List[String], listaPrecedenti: List[Int]): Unit = {
    //Passo base
    if (transazione.nonEmpty) {
      //Prendiamo il primo item della transazione
      val item = transazione.head
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val (node, flagNewNode) = lastNode.add(item)

      //Controlliamo se il nodo ha una profondità maggiore o uguale a 2, non è il primo nodo
      if (listaPrecedenti.nonEmpty) {
        //Aggiorniamo la matrice incrementando di uno le occorrenze di tale item
        aggiornaMatrice(matrix, listaPrecedenti, item, 1)
      }

      //Lista degli item visti in precedenza scorrendo la transazione in questione
      val newListaPrecedenti = listaPrecedenti :+ headerTable(item)._2

      //Se abbiamo aggiunto un nuovo nodo all'albero, aggiungiamo il nuovo nodo anche all'ht
      if (flagNewNode) {
        val valueItem = headerTable(item)
        headerTable = headerTable + (item -> (valueItem._1, valueItem._2, valueItem._3 :+ node))
      }
      //Iteriamo sul resto della transazione
      addNodeTransaction(node, transazione.tail, newListaPrecedenti)
    }
  }

  //Aggiornamento delle occorrenze all'interno della matrice
  def aggiornaMatrice(matrice: ListMap[String, Array[Int]], listaPrecedenti: List[Int], item: String, count: Int): Unit = {
    val array = matrice(item)
    //Dato un item aggiorniamo le occorrenze in base alla lista degli item precedenti
    listaPrecedenti.foreach(index => array(index) += count)
  }

  //Aggiungiamo tutti i path agli alberi condizionali
  @tailrec
  final def addPaths(sortedPaths: List[(List[String], Int)]): Unit = {
    if (sortedPaths.nonEmpty) {
      //Viene preso il primo path
      val head = sortedPaths.head
      //Viene inserito il path nel Conditional FPTree
      addNodePath(root, head._1, head._2, List[Int]())
      //Una volta aggiunto un nuovo path continuiamo con i successivi
      addPaths(sortedPaths.tail)
    }
  }

  //Aggiungiamo i nodo di un path all'albero
  @tailrec
  private def addNodePath(lastNode: Node[String], path: List[String], countPath: Int, listaPrecedenti: List[Int]): Unit = {

    if (path.nonEmpty) {
      val item = path.head
      //Aggiungiamo all'ultimo nodo creato il nuovo, passando il suo numero di occorrenze
      val (node, flagNewNode) = lastNode.add(item, countPath)

      //Viene controllato se sono presenti altri branch
      if (!moreBranch && lastNode.sons.size > 1) moreBranch = true

      // Viene aggiornata la lista dei precedenti e la matrice
      val newListaPrecedenti = {
        if (matrix.nonEmpty) {
          if (listaPrecedenti.nonEmpty) {
            aggiornaMatrice(matrix, listaPrecedenti, item, countPath)
          }
          listaPrecedenti :+ headerTable(item)._2
        } else {
          listaPrecedenti
        }
      }

      //Se abbiamo aggiunto un nuovo nodo, lo aggiungiamo anche alla linked list dell'ht
      if (flagNewNode) {
        val valueItem = headerTable(item)
        headerTable = headerTable + (item -> (valueItem._1, valueItem._2, valueItem._3 :+ node))
      }

      //Iteriamo sul resto del path
      addNodePath(node, path.tail, countPath, newListaPrecedenti)
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

  //Restituisce tutti i path che portano a quell'item
  def getAllPathsFromItem(item: String): List[(List[String], Int)] = {
    headerTable(item)._3.map(node => (getPathNode(node, List[String]()), node.occurrence))
  }

  def getHt: ListMap[String, (Int, Int, List[Node[String]])] = {
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

  //Restituisce gli item frequenti dalla matrice dato un item
  def getFreqItems(item: String): List[Int] = {
    if (matrix.contains(item))
      matrix(item).toList
    else List[Int]()
  }


  def merge(other: Tree): TreeStar = {

    def mergeVertical(toAdd: Node[String], last: Node[String]): Unit = {

      val item = toAdd.value

      val (newLastNode, flagNewNode) = last.add(item, toAdd.occurrence)

      if (flagNewNode) {
        val valueItem = headerTable(item)
        headerTable = headerTable + (item -> (valueItem._1, valueItem._2, valueItem._3 :+ newLastNode))
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