package demo

import utils.Utils._

import scala.annotation.tailrec
import mainClass.MainClass.minSupport

object AprioriDemo extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val (dataset, dimDataset) = prendiDataset()

  println("-----------------------------------")
  println("Dataset:\n" + dataset.mkString("\n"))
  println("-----------------------------------")

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[Set[String]]): Map[Set[String], Int] = {
    val itemCount = item.map(x => x -> dataset.count(y => x.subsetOf(y))).toMap
    println("Conteggio occorrenze item:\n" + itemCount.mkString("\n"))
    println("-----------------------------------")

    itemCount
  }

  // Filtraggio degli itemset, vengono eliminati se i subset non sono itemset frequenti, poi vengono eliminati anche filtrati
  // (e dunque eliminati) in base al numero di occorrenze nelle transazioni
  def prune(candidati: List[Set[String]], lSetkeys: List[Set[String]]) = {
    if (candidati.head.size > 2) {
      val afterPrune = candidati.filter(x => x.subsets(x.size - 1).forall(y => lSetkeys.contains(y)))
      println("Itemset candidati dopo la prune:\n" + afterPrune.mkString("\n"))
      println("-----------------------------------")
      afterPrune
    } else candidati
  }

  //Parte iterativa dell'algoritmo
  @tailrec
  def aprioriIter(mapItems: Map[Set[String], Int], setSize: Int): Map[Set[String], Int] = {
    //Ultimi elementi aggiunti
    val lastElements = mapItems.keys.filter(x => x.size == (setSize - 1))
    //Creazione degli itemset candidati (Item singoli + combinazioni)
    val setsItem = lastElements.reduce((x, y) => x ++ y).subsets(setSize).toList
    if (setsItem.nonEmpty) {
      println("Itemset candidati passo " + setSize + ":\n" + setsItem.mkString("\n"))
      println("-----------------------------------")
      //Eliminazione degli itemset non frequenti con il metodo prune
      val itemsSetCountFilter = countItemSet(prune(setsItem, lastElements.toList)).filter(x => x._2 >= minSupport)
      println("Itemset candidati passo " + setSize + " dopo il filtro:\n" + itemsSetCountFilter.mkString("\n"))
      println("-----------------------------------")
      //Controllo che la mappa relativa creata con gli itemset non sia vuota, se è vuota l'algoritmo è terminato
      if (itemsSetCountFilter.nonEmpty) {
        aprioriIter(mapItems ++ itemsSetCountFilter, setSize + 1)
      } else {
        mapItems
      }
    } else {
      mapItems
    }
  }

  //Calcolo di tutti i singoli item
  val totalItem = (dataset reduce ((xs, x) => xs ++ x) map (x => Set(x))).toList

  //Esecuzione effettiva dell'algoritmo
  def exec() = {
    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }

  def avviaAlgoritmo():Map[Set[String], Int] = {
    //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= minSupport)

    println("Filtraggio item singoli:\n" + firstStep.mkString("\n"))
    println("-----------------------------------")

    aprioriIter(firstStep, 2)
  }
}
