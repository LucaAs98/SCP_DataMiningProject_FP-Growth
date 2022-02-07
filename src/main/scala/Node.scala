import scala.annotation.tailrec

class Node[T](var value: T, var sons: List[Node[T]]) extends Serializable {
  var occurrence: Int = {
    if (value == null || value == -1) -1 else 1
  }

  var padre: Node[T] = null

  def isHead: Boolean = occurrence == -1

  def isEmpty: Boolean = sons.isEmpty

  //Verifica che i figli di tale nodo contengano tale valore
  def contains(newElement: T): Boolean = sons.exists(n => n.value == newElement)

  def getNodeSon(name: T): Node[T] = {
    //Cerca il nodo nei figli e restituisce il nodo stesso
    val node = sons.find(p => p.value == name) match {
      case Some(value) => value
      case None => null //Non entra mai
    }
    node.occurrence += 1 //Aumenta la sua occorrenza
    node
  }

  def add(newValue: T): (Node[T], Boolean) = {
    //Se i figli non lo contengono già
    if (!this.contains(newValue)) {
      val newNode = new Node[T](newValue, List()) //Crea un nuovo nodo
      newNode.padre = this //Gli assegna il padre
      sons = sons :+ newNode //Al padre aggiungiamo il nuovo figlio
      (newNode, true) //Restituiamo il nodo appena creato e se esisteva già
    } else {
      (this.getNodeSon(newValue), false) //Restituiamo il nodo già esistente
    }
  }

  def merge(other: Node[T]): Node[T] = {

    def mergeVertical(toAdd: Node[T], last: Node[T]): Unit = {
      val newLastNode = last.add(toAdd.value)
      mergeHorizontal(toAdd.sons, newLastNode._1)
    }

    @tailrec
    def mergeHorizontal(listSons: List[Node[T]], last: Node[T]): Node[T] = {
      if (listSons.nonEmpty) {
        val head = listSons.head
        mergeVertical(head, last)
        mergeHorizontal(listSons.tail, last)
      }
      else
        last
    }

    mergeHorizontal(other.sons, this)
  }

  override def toString: String = {
    "Value: " + this.value + " Occorrenze: " + this.occurrence
  }

  /*def getAllValues(acc: List[T]): List[T] = {
    if (!this.isEmpty) {
      val newAcc = acc :+ this.sons.head.value
      this.sons.head.getAllValues(newAcc)
    } else
      acc
  }*/
}

