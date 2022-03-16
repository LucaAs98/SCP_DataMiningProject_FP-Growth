package fpgrowthmod

import classes.Tree
import utils.Utils._

import scala.collection.immutable.ListMap
import mainClass.MainClass.minSupport

object FPGrowthModPar extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val (datasetAux, dimDataset) = prendiDataset()
  val dataset = datasetAux.par

  //Elementi singoli presenti nel dataset
  val totalItem = dataset.reduce((xs, x) => xs ++ x).toList

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[String]): Map[String, Int] = {
    item.map(x => x -> dataset.count(y => y.contains(x))).toMap
  }

  //Ordiniamo le transazioni in modo decrescente
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


  def calcListSubsets(item: String, oneCondPatt: List[(List[String], Int)]) = {
    /* Creiamo la lista dei frequentItemset, mantenendo quelli duplicati, ma con diverse occorrenze. In questo modo possiamo
    * parallelizzare questo passaggio, invece di eseguirlo ricorsivamente su ogni path. */
    oneCondPatt.par.flatMap(path => path._1.toSet.subsets().toList.par.map(elem => (elem + item) -> path._2).filter(_._1.nonEmpty).toList)
  }

  //Esecuzione effettiva dell'algoritmo
  def exec() = {
    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }

  def avviaAlgoritmo():Map[Set[String], Int] = {
    //totalItems che rispettano il minSupport
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio

    //Ordina gli item dal più frequente al meno
    val firstMapSorted = ListMap(firstStep.toList.sortWith((elem1, elem2) => functionOrder(elem1, elem2)): _*)

    //Ordiniamo le transazioni del dataset in modo decrescente
    val orderDataset = datasetFilter(firstMapSorted.keys.toList).seq.toList

    //Creiamo il nostro albero vuoto
    val newTree = new Tree(firstMapSorted)

    //Aggiungiamo le transazioni al nuovo albero
    newTree.addTransactions(orderDataset)

    //Ordiniamo i singoli item in modo crescente per occorrenze e modo non alfabetico
    val singleElementsCrescentOrder = ListMap(firstMapSorted.toList.reverse: _*)

    //Creazione conditional pattern base, per ogni nodo prendiamo i percorsi in cui quel nodo è presente
    val conditionalPatternBase = singleElementsCrescentOrder.map(x => x._1 -> newTree.getAllPathsFromItem(x._1)).par

    /* Calcoliamo il nostro risultato finale. Dopo che otteniamo la lista di tutti i frequent itemset (anche duplicati)
    * sommiamo le occorrenze di quelli uguali per ottenere i freq itemset finali. */
    val frequentItemSet = conditionalPatternBase.map(elem => calcListSubsets(elem._1, elem._2)).flatten.groupBy(_._1)
      .map(elem => elem._1 -> elem._2.map(_._2).sum).filter(_._2 >= minSupport)

    frequentItemSet.seq
  }
}