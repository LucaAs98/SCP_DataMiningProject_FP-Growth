package classes

import scala.annotation.tailrec
import scala.collection.parallel.ParMap

class ArrayTrie(trie: Trie) extends Serializable {

  val itemMap: Map[String, (Int, Int)] = trie.getItemMap
  /* Inizializzazione dell'array in cui sono contenuti gli indici, che indicano da dove iniziano le celle contigue
    * per ogni item nell'arrayTrie. */
  val startIndex: Array[Int] = calcStartIndex()
  //Array ce rappresenta il trie
  val arrayTrie: Array[(String, Int, Int)] = createArrayTrie()

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
  final def getPercorso(parent: Int, accPath: List[String]): List[String] = {
    //Se il nodo non è figlio diretto della radice
    if (parent != -1) {
      val last = arrayTrie(parent) //Prendiamo la cella contenente il padre
      getPercorso(last._3, last._1 :: accPath) //Aggiungiamo il padre all'accumulatore del path
    } else {
      List.empty[String] ::: accPath
    }
  }

  //Calcoliamo startIndex partendo dal conto dei nodi di un certo item
  def calcStartIndex(): Array[Int] = {
    val startIndexApp = new Array[Int](itemMap.size)
    //Riempiamo le celle dello startIndex partendo da quella in posizione 1
    for (i <- 1 until startIndexApp.length) {
      //Prendiamo quanti nodi esistono dell'item precedente e li sommiamo all'indice di partenza di esso
      startIndexApp(i) = trie.counterDifferentNode(i - 1) + startIndexApp(i - 1)
    }
    startIndexApp
  }

  //Creazione del trie 
  def createArrayTrie(): Array[(String, Int, Int)] = {
    val aTrie = new Array[(String, Int, Int)](trie.nodeCounter)
    createArrayTrieRec(aTrie, trie.root, -1)
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
      createArrayTrieRec(aTrie, firstSon, parentIndex)
      //Aggiorniamo l'array per ogni figlio
      createTrieSons(aTrie, sons.tail, parentIndex)
    }
  }

  //Riempiamo l'arrayTrie
  def createArrayTrieRec(aTrie: Array[(String, Int, Int)], lastNode: NodeTrie, parentIndex: Int): Unit = {
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

  //Creazione di un percorso dato un nodo
  @tailrec
  private def itCreateCondPB(parent: Int, accPath: List[String]): List[String] = {
    if (parent != -1) {
      val last = arrayTrie(parent)
      itCreateCondPB(last._3, last._1 :: accPath)
    } else {
      List.empty[String] ::: accPath
    }
  }

  //Creiamo il conditionalPB per tutti i nodi, scorriamo l'arrayTrie
  def createCondPB(): Map[String, List[(List[String], Int)]] = {
    val mapItemPath = arrayTrie.map(elem => elem._1 ->
      (itCreateCondPB(elem._3, List.empty[String]), elem._2)).groupBy(_._1)

    //Formattiamolo a nostro piacimento
    mapItemPath.map(elem => elem._1 -> elem._2.map(_._2).toList)
  }

  //Creiamo il conditionalPB in parallelo
  def createCondPBPar(): ParMap[String, List[(List[String], Int)]] = {
    val mapItemPath = arrayTrie.par.map(elem => elem._1 ->
      (itCreateCondPB(elem._3, List.empty[String]), elem._2)).groupBy(_._1)

    //Formattiamolo a nostro piacimento
    mapItemPath.map(elem => elem._1 -> elem._2.map(_._2).toList)
  }
}