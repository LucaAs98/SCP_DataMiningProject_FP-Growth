import java.io.{BufferedWriter, File, FileWriter}

import scala.annotation.tailrec
import scala.collection.parallel.CollectionConverters._

object AprioriPar extends App {
  //Scelta dataset (Csv e txt identitici)
  val file = "src/main/resources/dataset/datasetKaggleAlimenti.txt"
  val dataset = datasetFromFile(file)

  //supporto minimo
  def min_sup = 30

  //Metodo per la lettura del dataset
  def datasetFromFile(nfile: String): List[Set[String]] = {
    //Creazione file
    val source = io.Source.fromFile(nfile)

    //Vengono prese le linee del file e separate, creando una list di set di stringhe
    val data = source.getLines().map(x => x.split(",").toSet).toList
    source.close()
    data
  }

  //Passando la lista dei set degli item creati, conta quante volte c'è l'insieme nelle transazioni
  def countItemSet(item: List[Set[String]]):Map[Set[String], Int] = {
    ((item.par map (x => x -> (dataset count (y => x.subsetOf(y))))).seq.toMap)
  }

  //Filtraggio degli itemset, vengono eliminati se i subset non itemset frequenti, poi vengono eliminati anche filtrati
  // (e dunque eliminati) in base al numero di occorrenze nelle transazioni
  def prune(map: Map[Set[String], Int], lSetkeys: List[Set[String]]) = {
     (map filter (x => x._1.subsets(x._1.size - 1) forall (y => lSetkeys.contains(y)))) filter (x => x._2 >= min_sup)
  }

  //Parte iterativa dell'algoritmo
  @tailrec
  def aprioriIter(map: Map[Set[String], Int], l: List[Map[Set[String], Int]], setSize: Int): List[Map[Set[String], Int]] = {

    //Controllo che la mappa passata non sia vuota
    if (map.isEmpty) {
      println(l.size)
      println(l.last.head._1.size)
      println(l.last.toSeq.sortBy(_._2))
      l
    }
    else {
      //Creazione degli itemset candidati (Item singoli + combinazioni)
      val setsItem = ((map foldLeft (Set[String]())) ((xs, x) => x._1 ++ xs)).toList.combinations(setSize)
      println("Calcolo combinazioni! effettuato")

      //Eliminazione degli itemset non frequenti con il metodo prune
      val c = prune((countItemSet(setsItem.toList map (x => x.toSet))), l.last.keys.toList)
      println("Prune effettuato!!!!!")

      //Controllo che la mappa relativa creata con gli itemset non sia vuota, se è vuota l'algoritmo è terminato
      if (c.nonEmpty) {
        println(l.size)
        println(l.last.head._1.size)
        println(l.last.toSeq.sortBy(_._2))
        aprioriIter(c, l :+ c, setSize + 1)
      }
      else {
        l
      }
    }
  }

  //Calcolo di tutti i sinogli item
  def totalItem = ((dataset foldLeft (Set[String]())) ((xs, x) => xs ++ x) map (x => Set(x))).toList


  //Esecuzione effettiva dell'algoritmo
  def exec() ={
    val firstStep = time(countItemSet(totalItem).filter(x => x._2 >= min_sup))
     //Primo passo, conteggio delle occorrenze dei singoli item con il filtraggio
    /*val listOfTables = */aprioriIter(firstStep, List(firstStep), 2)

  }

  def time[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000 + "ms")
    result
  }

  //Scrittura dei file
  def writeFile(filename: String, s: List[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    s foreach(x=> bw.write(x))
    bw.close()
  }

  //Calcolo delle confidenze
  def calcoloConfidenza(setItems: Set[String], listOfTables: List[Map[Set[String], Int]]) = {
    val length = setItems.size //numero degli item nel set
    val nTran = dataset.size //numero di transazioni

    //Numero occorrenze dell'intero itemset
    val supportIS = listOfTables(length - 1).get(setItems) match {
      case None => 0
      case Some(int) => int
    }

    //calcolo dei subset del set su cui dobbiamo fare i calcoli
    val subsets = setItems.subsets(setItems.size - 1).toList

    //Calcolo dei subsets del set passato come parametro
    val supportSubset = (subsets map (x => x -> (listOfTables(x.size - 1).get(x) match {
      case None => 0
      case Some(int) => int
    })))

    //Viene ricavato il numero delle occorrenze di ogni singolo subset
    val totalSingleItem = (setItems map (x => Set(x) -> (listOfTables.head.get(Set(x)) match {
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
  val pollop = (lo flatMap ( x => (x map ( y=> y._1.toString() + "->" + y._2 + "\n")).toList))
  writeFile("src/main/resources/results/AprioriParResult.txt",pollop )

  //Stampa dei supporti vari
  val pollop2 = ((lo).tail flatMap (x => x.keys.toList)).flatMap(x => calcoloConfidenza(x, lo))
  writeFile("src/main/resources/results/AprioriParConfidenza.txt", pollop2)
}
