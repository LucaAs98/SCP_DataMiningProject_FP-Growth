import scala.annotation.tailrec
import scala.collection.parallel.ParMap

class ArrayTrie(trie: Trie) extends Serializable {



  val itemMap: Map[String, (Int, Int)] = trie.getItemMap
  val startIndex: Array[Int] = calcStartIndex()
  val arrayTrie: Array[(String, Int, Int)] = createTrie()

  def getArrayTrie: Array[(String, Int, Int)] = {
    arrayTrie
  }

  def getStartIndex: Array[Int] = {
    startIndex
  }

  def getIndexToItem: Map[Int, String] = {
    itemMap.map(elem => elem._2._1 -> elem._1)
  }

  def getNodesItem(indexItem: Int, numNodiDiff: Int): Array[(String, Int, Int)] = {
    arrayTrie.slice(startIndex(indexItem), startIndex(indexItem) + numNodiDiff)
  }

  //Creazione di un percorso dato un elemento, simile a listaPercorsi, ma per l'arrayTrie
  @tailrec
  final def getPercorso(parent: Int, acc: List[String]): List[String] = {
    if (parent != -1) {
      val last = arrayTrie(parent)
      getPercorso(last._3, (last._1) :: acc)
    } else {
      List.empty[String] ::: acc
    }
  }

  //Calcoliamo startIndex partendo dal conto dei nodi di un certo item
  def calcStartIndex(): Array[Int] = {
    val app = new Array[Int](itemMap.size)
    //Riempiamo le celle dello startIndex partendo da quella in posizione 1
    for (i <- 1 until app.length) {
      //Prendiamo quanti nodi esistono dell'item precedente e li sommiamo all'indice di partenza di esso
      app(i) = trie.counterDifferentNode(i - 1) + app(i - 1)
    }
    app
  }

  def createTrie(): Array[(String, Int, Int)] = {
    val aTrie = new Array[(String, Int, Int)](trie.nodeCounter)
    createTrieRec(aTrie, trie.root, -1)
    aTrie
  }


  //Mettiamo l'elemento nell'arrayTrie
  @tailrec
  private def putElementInArray(aTrie: Array[(String, Int, Int)], index: Int, node: NodeTrie, parent: Int): Int = {
    //Se la posizione in cui doobiamo iniziare a mettere gli item con tale valore non è occupata, lo inseriamo
    if (aTrie(index) == null) {
      aTrie(index) = (node.value, node.occurrence, parent)
      //Rerstituiamo l'indice in modo tale da ricavarci quale sarà il padre del nodo successivo (dato che è suo figlio)
      index
    } else {
      //Se la cella è già occupata, andiamo alla successiva
      putElementInArray(aTrie, index + 1, node, parent)
    }
  }

  //Aggiorniamo l'arrayTrie per ogni figlio
  @tailrec
  private def createTrieSons(aTrie: Array[(String, Int, Int)], sons: List[NodeTrie], parentIndex: Int): Unit = {
    //Scorriamo tutti i figli
    if (sons.nonEmpty) {
      val firstSon = sons.head //Prendiamo il primo figlio
      //Aggiorniamo l'array anche per ogni figlio dei figli
      createTrieRec(aTrie, firstSon, parentIndex)
      //Aggiorniamo l'array per ogni figlio
      createTrieSons(aTrie, sons.tail, parentIndex)
    }
  }

  //Riempiamo l'arrayTrie
  def createTrieRec(aTrie: Array[(String, Int, Int)], lastNode: NodeTrie, parentIndex: Int): Unit = {
    //Se non è la radice dell'albero
    if (!lastNode.isHead) {
      //Prendiamo l'indice "di ordinamento" dell'item
      val index = itemMap(lastNode.value)._1
      //Prendiamo l'indice di partenza dell'item
      val firstIndex = startIndex(index)
      //Mettiamo l'elemento nell'arrayTrie e restituiamo il padre del nodo successivo che andremo ad inserire
      val newParent = putElementInArray(aTrie, firstIndex, lastNode, parentIndex)
      //Aggiungiamo all'array anche i figli
      createTrieSons(aTrie, lastNode.sons, newParent)
    } else {
      //Se è la radice dobbiamo aggiornare l'arrayTrie per i figli della radice
      createTrieSons(aTrie, lastNode.sons, parentIndex)
    }
  }

  //Creazione di un percorso dato un elemento, simile a listaPercorsi, ma per l'arrayTrie
  @tailrec
  private def itCreateConditionalPatternBase(parent: Int, acc: List[String]): List[String] = {
    if (parent != -1) {
      val last = arrayTrie(parent)
      itCreateConditionalPatternBase(last._3, (last._1) :: acc)
    } else {
      List.empty[String] ::: acc
    }
  }

  //Creiamo il conditionalPB
  def createCondPB(): Map[String, List[(List[String], Int)]] = {
    val mapItemPath = arrayTrie.map(elem => elem._1 ->
      (itCreateConditionalPatternBase(elem._3, List.empty[String]), elem._2)).groupBy(_._1)
    mapItemPath.map(elem => elem._1 -> elem._2.map(_._2).toList)
  }

  //Creiamo il conditionalPB
  def createCondPBPar(): ParMap[String, List[(List[String], Int)]] = {
    val mapItemPath = arrayTrie.par.map(elem => elem._1 ->
      (itCreateConditionalPatternBase(elem._3, List.empty[String]), elem._2)).groupBy(_._1)
    val mapItemPathMapped = mapItemPath.map(elem => elem._1 -> elem._2.map(_._2).toList)
    mapItemPathMapped
  }


}
