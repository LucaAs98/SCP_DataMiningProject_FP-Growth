class Node(var value: String, var sons: List[Node]) {
  var occurrence: Int = {
    if (value == null) -1 else 1
  }

  var padre: Node = null

  def isHead: Boolean = occurrence == -1

  def isEmpty: Boolean = sons.isEmpty

  //Verifica che i figli di tale nodo contengano tale valore
  def contains(newElement: String): Boolean = sons.exists(n => n.value == newElement)

  def getNodeSon(name: String): Node = {
    //Cerca il nodo nei figli e restituisce il nodo stesso
    val node = sons.find(p => p.value == name) match {
      case Some(value) => value
      case None => null //Non entra mai
    }
    node.occurrence += 1 //Aumenta la sua occorrenza
    node
  }

  def add(newValue: String): (Node, Boolean) = {
    //Se i figli non lo contengono già
    if (!this.contains(newValue)) {
      val newNode = new Node(newValue, List()) //Crea un nuovo nodo
      newNode.padre = this //Gli assegna il padre
      sons = sons :+ newNode //Al padre aggiungiamo il nuovo figlio
      (newNode, true) //Restituiamo il nodo appena creato e se esisteva già
    } else {
      (this.getNodeSon(newValue), false) //Restituiamo il nodo già esistente
    }
  }

  override def toString: String = {
    "Value: " + this.value + " Occorrenze: " + this.occurrence
  }
}

