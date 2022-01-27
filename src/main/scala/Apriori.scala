import java.io.{BufferedWriter, File, FileWriter}

import scala.annotation.tailrec
import scala.io.Source

object Apriori extends App {
  //Scelta dataset (Csv e txt identitici)
  val file = "src/main/resources/dataset/datasetKaggleAlimenti.txt"
  val dataset = datasetFromFile(file)

  //supporto minimo
  def min_sup = 30

  //Metodo per la lettura del dataset
  def datasetFromFile(nfile: String): List[Set[String]] = {
    //Creazione file
    val source = Source.fromFile(nfile)

    //Vengono prese le linee del file e separate, creando una list di set di stringhe
    val data = source.getLines().map(x => x.split(",").toSet).toList
    source.close()
    data
  }

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
    val itemsSetCountFilter = countItemSet(prune(setsItem, lastElemtents.toList)) filter (x => x._2 >= min_sup)
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
    val firstStep = countItemSet(totalItem).filter(x => x._2 >= min_sup) //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    aprioriIter(firstStep, 2)
  }

  def time[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  //Scrittura dei file
  def writeFile(filename: String, s: List[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    s foreach (x => bw.write(x))
    bw.close()
  }

  //Calcolo delle confidenze
  def calcoloConfidenza(setItems: Set[String], listOfTables: Map[Set[String], Int]) = {
    val length = setItems.size //numero degli item nel set
    val nTran = dataset.size //numero di transazioni

    //Numero occorrenze dell'intero itemset
    val supportIS = listOfTables.get(setItems) match {
      case None => 0
      case Some(int) => int
    }

    //calcolo dei subset del set su cui dobbiamo fare i calcoli
    val subsets = setItems.subsets(setItems.size - 1).toList

    //Calcolo dei subsets del set passato come parametro
    val supportSubset = (subsets map (x => x -> (listOfTables.get(x) match {
      case None => 0
      case Some(int) => int
    })))

    //Viene ricavato il numero delle occorrenze di ogni singolo subset
    val totalSingleItem = (setItems map (x => Set(x) -> (listOfTables.get(Set(x)) match {
      case None => 0
      case Some(int) => int
    }))).toMap

    //Creazione delle stringhe
    val p = supportSubset map (x => "antecedente: " + x._1.toString() +
      " successivo: " + setItems.--(x._1).toString() + " supporto antecedente: " + x._2.toFloat / nTran + " supporto successivo: "
      + (totalSingleItem.get(setItems.--(x._1)) match {
      case None => 0
      case Some(int) => int
    }).toFloat / nTran + " supporto: " + (supportIS.toFloat / nTran) +
      " confidence: " + (supportIS.toFloat / x._2) + "\n")

    p
  }

  val lo = time(exec())
  //stampa della tabella con tutte le occorrenze per tutti gli itemset
  val pollop = (lo map (y => y._1.toString() + "->" + y._2 + "\n")).toList
  writeFile("src/main/resources/results/AprioriResult.txt", pollop)

  //Stampa dei supporti vari
  val pollop2 = lo.filter(x => x._1.size > 1).keys.toList.flatMap(x => calcoloConfidenza(x, lo))
  writeFile("src/main/resources/results/AprioriConfidenza.txt", pollop2)
}
