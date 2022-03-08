package classes

class NodeTrie(var value: String, var sons: List[NodeTrie]) extends Serializable {
  var occurrence: Int = {
    if (value == null || value == -1) -1 else 1
  }

  def isHead: Boolean = occurrence == -1

  def isEmpty: Boolean = sons.isEmpty

  //Verifica che i figli di tale nodo contengano tale valore
  def contains(newElement: String): Boolean = sons.exists(n => n.value == newElement)

  def getNodeSon(name: String): NodeTrie = {
    //Cerca il nodo nei figli e restituisce il nodo stesso
    val node = sons.find(p => p.value == name) match {
      case Some(value) => value
      case None => null //Non entra mai
    }
    node.occurrence += 1 //Aumenta la sua occorrenza
    node
  }

  def getNodeSon(name: String, count: Int): NodeTrie = {
    //Cerca il nodo nei figli e restituisce il nodo stesso
    val node = sons.find(p => p.value == name) match {
      case Some(value) => value
      case None => null //Non entra mai
    }
    node.occurrence += count //Aumenta la sua occorrenza
    node
  }

  def add(newValue: String): (NodeTrie, Boolean) = {
    //Se i figli non lo contengono già
    if (!this.contains(newValue)) {
      val newNode = new NodeTrie(newValue, List()) //Crea un nuovo nodo
      sons = sons :+ newNode //Al padre aggiungiamo il nuovo figlio
      (newNode, true) //Restituiamo il nodo appena creato e se esisteva già
    } else {
      (this.getNodeSon(newValue), false) //Restituiamo il nodo già esistente
    }
  }

  def add(newValue: String, count: Int): (NodeTrie, Boolean) = {
    //Se i figli non lo contengono già
    if (!this.contains(newValue)) {
      val newNode = new NodeTrie(newValue, List()) //Crea un nuovo nodo
      newNode.occurrence = count
      sons = sons :+ newNode //Al padre aggiungiamo il nuovo figlio
      (newNode, true) //Restituiamo il nodo appena creato e se esisteva già
    } else {
      (this.getNodeSon(newValue, count), false) //Restituiamo il nodo già esistente
    }
  }

  override def toString: String = {
    "Value: " + this.value + " Occorrenze: " + this.occurrence
  }

}

