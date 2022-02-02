import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import java.io.{BufferedWriter, File, FileWriter}

object AprioriRDD extends App {
  val conf = new SparkConf().setAppName("AprioriRDD").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val file = "src/main/resources/dataset/datasetKaggleAlimenti100.txt"
  val dataset = sc.textFile(file)
  val min_sup = 30

  //Valuta il tempo di un'espressione
  def time[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    //println("Tempo di esecuzione: " + (t1 - t0) / 1000000 + "ms")
    (result, (t1 - t0) / 1000000)
  }

  def gen(itemSets: RDD[Set[String]], size: Int): List[Set[String]] = {
    (itemSets reduce ((x, y) => x ++ y)).subsets(size).toList
  }

  def prune(itemCandRDD: RDD[Set[String]], itemsSetPrec: Array[Set[String]]) = {
    (itemCandRDD filter (x => x.subsets(x.size - 1) forall (y => itemsSetPrec.contains(y)))).collect()
  }

  def listOfPairs(str: String, itemSets: Array[Set[String]], size: Int) = {
    val items = str.split(",").toSet
    if (items.size >= size) {
      (itemSets.filter(y => y.subsetOf(items)))
    }
    else {
      Array[Set[String]]()
    }
  }
  //dafinire
  def countItemSet(itemSets: Array[Set[String]], size: Int) = {
    val ciao = dataset.flatMap(x => listOfPairs(x, itemSets, size))
    val itemPairs = ciao.map(x => (x, 1))
    val itemCounts = itemPairs.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= min_sup)
    itemCounts.collect().toMap
  }

  @tailrec
  def aprioriIter(mapItem: Map[Set[String], Int], dim: Int):Map[Set[String], Int] = {
    val itemsSetPrec = sc.parallelize(mapItem.keys.filter(x => x.size == (dim - 1)).toList) //vedere se cambia qualcosa con persist o meno
    val candidati = gen(itemsSetPrec, dim)
    val itemSets = prune(sc.parallelize(candidati), itemsSetPrec.collect())
    val itemSetCounts= countItemSet(itemSets, dim).filter(_._2 >= min_sup)

    if(itemSetCounts.isEmpty){
      mapItem
    }
    else
      {
        aprioriIter(mapItem ++ itemSetCounts, dim+1)
      }
  }

  def exec() = {
    val items = dataset.flatMap(x => x.split(","))
    val itemPairs = items.map(x => (Set(x), 1))
    val itemCounts = itemPairs.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= min_sup)
    val k = itemCounts.collect().toMap

    if (k.isEmpty) {
      k
    }
    else {
      aprioriIter(k, 2)
      //p.toList.sortBy(_._2).foreach(println(_))
      //println(p.size)
    }
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
    val nTran = dataset.count() //numero di transazioni

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

  println("Ciaooooooo")
  val (lo, tempo) = time(exec())
  println("Ciaooooooo22222222222")
  //stampa della tabella con tutte le occorrenze per tutti gli itemset
  val pollop = (lo map (y => y._1.toString() + "->" + y._2 + "\n")).toList
  writeFile("src/main/resources/results/AprioriRDDResult.txt", pollop)

  //Stampa dei supporti vari
  val pollop2 = lo.filter(x => x._1.size > 1).keys.toList.flatMap(x => calcoloConfidenza(x, lo))
  writeFile("src/main/resources/results/AprioriRDDConfidenza.txt", pollop2)



}
