import scala.annotation.tailrec

object Apriori extends App {
  //Scelta dataset (Csv e txt identitici)
  val dataset = Utils.prendiDataset("datasetKaggleAlimenti.txt")

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[Set[String]]): Map[Set[String], Int] = {
    (item map (x => x -> (dataset count (y => x.subsetOf(y))))).toMap
  }

  //Filtraggio degli itemset, vengono eliminati se i subset non itemset frequenti, poi vengono eliminati anche filtrati
  // (e dunque eliminati) in base al numero di occorrenze nelle transazioni
  def prune(candidati: List[Set[String]], lSetkeys: List[Set[String]]) = {
    candidati filter (x => x.subsets(x.size - 1) forall (y => lSetkeys.contains(y)))
  }

  //Parte iterativa dell'algoritmo
  @tailrec
  def aprioriIter(mapItems: Map[Set[String], Int], setSize: Int): Map[Set[String], Int] = {

    val lastElemtents = mapItems.keys.filter(x => x.size == (setSize - 1))

    //Creazione degli itemset candidati (Item singoli + combinazioni)
    val setsItem = lastElemtents.reduce((x, y) => x ++ y).subsets(setSize).toList
    //Eliminazione degli itemset non frequenti con il metodo prune
    val itemsSetCountFilter = countItemSet(prune(setsItem, lastElemtents.toList)) filter (x => x._2 >= Utils.minSupport)
    //Controllo che la mappa relativa creata con gli itemset non sia vuota, se è vuota l'algoritmo è terminato
    if (itemsSetCountFilter.nonEmpty) {
      aprioriIter(mapItems ++ itemsSetCountFilter, setSize + 1)
    }
    else {
      mapItems
    }
  }

  //Calcolo di tutti i sinogli item
  //def totalItem = time((((dataset foldLeft (Set[String]())) ((xs, x) => xs ++ x)) map (x => Set(x))).toList)
  val totalItem = (dataset reduce ((xs, x) => xs ++ x) map (x => Set(x))).toList

  //Esecuzione effettiva dell'algoritmo
  def exec() = {
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= Utils.minSupport) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    aprioriIter(firstStep, 2)
  }

  val result = Utils.time(exec())

  Utils.scriviSuFileFrequentItemSet(result, dataset.size.toFloat, "AprioriResult.txt")
  Utils.scriviSuFileSupporto(result, dataset.size.toFloat, "AprioriConfidenzaResult.txt")
}