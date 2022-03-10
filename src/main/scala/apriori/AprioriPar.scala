package apriori

import scala.annotation.tailrec
import utils.Utils._

object AprioriPar extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset().par

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[Set[String]]): Map[Set[String], Int] = {
    item.par.map(x => x -> dataset.count(y => x.subsetOf(y))).seq.toMap
  }

  //Filtraggio degli itemset, vengono eliminati se i subset non itemset frequenti, poi vengono eliminati anche filtrati
  // (e dunque eliminati) in base al numero di occorrenze nelle transazioni
  def prune(candidati: List[Set[String]], lSetkeys: List[Set[String]]) = {
    if (candidati.head.size > 2) {
      candidati.filter(x => x.subsets(x.size - 1).forall(y => lSetkeys.contains(y)))
    } else candidati
  }

  //Parte iterativa dell'algoritmo
  @tailrec
  def aprioriIter(mapItems: Map[Set[String], Int], setSize: Int): Map[Set[String], Int] = {
    val lastElements = mapItems.keys.filter(x => x.size == (setSize - 1))

    //Creazione degli itemset candidati (Item singoli + combinazioni)
    val setsItem = lastElements.par.reduce((x, y) => x ++ y).subsets(setSize).toList
    if (setsItem.nonEmpty) {
      //Eliminazione degli itemset non frequenti con il metodo prune
      val itemsSetCountFilter = countItemSet(prune(setsItem, lastElements.toList)).filter(x => x._2 >= minSupport)
      //Controllo che la mappa relativa creata con gli itemset non sia vuota, se è vuota l'algoritmo è terminato
      if (itemsSetCountFilter.nonEmpty) {
        aprioriIter(mapItems ++ itemsSetCountFilter, setSize + 1)
      }
      else {
        mapItems
      }
    } else mapItems
  }

  //Calcolo di tutti i sinogli item
  def totalItem = dataset.reduce((xs, x) => xs ++ x).map(x => Set(x)).toList

  //Esecuzione effettiva dell'algoritmo
  def exec() = {
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)
    //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    aprioriIter(firstStep, 2)
  }

  val result = time(exec())
  scriviSuFileFrequentItemSet(result, dataset.size.toFloat, "AprioriParResult.txt")
  scriviSuFileSupporto(result, dataset.size.toFloat, "AprioriParConfidenzaResult.txt")
}
