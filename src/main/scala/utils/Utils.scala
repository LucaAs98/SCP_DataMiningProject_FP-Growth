package utils

import classes.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object Utils {
  val mappaNomiFile: Map[Int, String] = Map[Int, String](
    0 -> "datasetKaggleAlimenti.txt",
    1 -> "datasetKaggleAlimenti10.txt",
    2 -> "datasetKaggleAlimenti100.txt",
    3 -> "T10I4D100K.txt",
    4 -> "datasetLettere.txt",
    5 -> "datasetLettere2.txt",
    6 -> "T40I10D100K.txt")

  val numFileDataset = 4
  val spazioVirgola = ","

  //Parametro di basket mining
  val minSupport = 2

  //Funzione per prendere il dataset dal file
  def prendiDataset(): List[Set[String]] = {
    val filePath = "src/main/resources/dataset/" + mappaNomiFile(numFileDataset)
    val file = new File(filePath)
    val source = Source.fromFile(file)
    val dataset = source.getLines().map(x => x.split(spazioVirgola).toSet).toList //Contenuto di tutto il file come lista
    source.close()
    dataset
  }

  //Funzione per prendere il dataset dal file
  def prendiDatasetInt(nomeFile: String): List[Set[Int]] = {
    val filePath = "src/main/resources/dataset/" + nomeFile
    val file = new File(filePath)
    val source = Source.fromFile(file)
    val dataset = source.getLines().map(x => x.split(",").toSet).toList //Contenuto di tutto il file come lista
    source.close()

    val distinctDataset = dataset.flatten.distinct.sortWith(_ < _).zipWithIndex.toMap

    dataset.map(x => x.map(distinctDataset(_)))
  }

  //Valuta il tempo di un'espressione
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Tempo di esecuzione: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  //Calcolo delle confidenze
  def calcoloConfidenza(singleResult: Set[String], numTransazioni: Float, result: Map[Set[String], Int]): Seq[String] = {

    //Numero occorrenze dell'intero itemset
    val supportIS = result.get(singleResult) match {
      case None => 0
      case Some(int) => int
    }

    //Calcolo dei subset del set su cui dobbiamo fare i calcoli
    val subsets = singleResult.subsets(singleResult.size - 1).toList

    //Calcolo dei subsets del set passato come parametro
    val supportSubset = (subsets map (x => x -> (result.get(x) match {
      case None => 0
      case Some(int) => int
    })))

    //Viene ricavato il numero delle occorrenze di ogni singolo subset
    val totalSingleItem = (singleResult map (x => Set(x) -> (result.get(Set(x)) match {
      case None => 0
      case Some(int) => int
    }))).toMap

    //Creazione delle stringhe
    val allSupportSubsetString = supportSubset map (x => "Antecedente: " + x._1.toString() +
      " Successivo: " + singleResult.--(x._1).toString() + " Supporto antecedente: " + x._2.toFloat / numTransazioni + " Supporto successivo: "
      + (totalSingleItem.get(singleResult.--(x._1)) match {
      case None => 0
      case Some(int) => int
    }).toFloat / numTransazioni + " Supporto: " + (supportIS.toFloat / numTransazioni) +
      " Confidence: " + (supportIS.toFloat / x._2))

    allSupportSubsetString
  }

  //Formatta il risultato ottenuto dalle computazioni in modo tale da calcolarne i frequentItemSet e lo salva su file
  def scriviSuFileFrequentItemSet(result: Map[Set[String], Int], numTransazioni: Float, nomeFile: String): Unit = {
    //Riordiniamo il risultato per visualizzarlo meglio sul file
    val resultOrdered1 = result.toSeq.sortBy(_._2).map(elem => elem._1 -> (elem._2, elem._2.toFloat / numTransazioni)).toList.map(elem => elem.toString())

    scrivi(resultOrdered1, nomeFile)
  }

  //Formatta il risultato ottenuto dalle computazioni in modo tale da calcolarne il supporto e lo salva su file
  def scriviSuFileSupporto(result: Map[Set[String], Int], numTransazioni: Float, nomeFile: String): Unit = {
    val result2 = result.filter(x => x._1.size > 1).keys.toList.flatMap(x => calcoloConfidenza(x, numTransazioni, result))

    scrivi(result2, nomeFile)
  }

  //Scrittura effettiva su file
  def scrivi(daScrivere: List[String], nomeFile: String): Unit = {
    val writingFile = new File("src/main/resources/results/" + nomeFile)
    val bw = new BufferedWriter(new FileWriter(writingFile))
    for (row <- daScrivere) {
      bw.write(row + "\n")
    }
    bw.close()
  }

  //Restituisce uno spark context
  def getSparkContext(nomeContext: String): SparkContext = {
    val conf = new SparkConf().setAppName(nomeContext).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }

  //Restituisce un RDD da file
  def getRDD(sc: SparkContext): RDD[String] = {
    val file = "src/main/resources/dataset/" + mappaNomiFile(numFileDataset)
    sc.textFile(file)
  }

  //Restituisce un RDD da file
  def getRDD(nomeFile: String, sc: SparkContext): RDD[String] = {
    val file = "src/main/resources/dataset/" + nomeFile
    sc.textFile(file)
  }

  //Metodo per la stampa dell'albero
  def printTree(tree: Node[String], str: String): Unit = {
    if (tree.occurrence != -1) {
      println(str + tree.value + " " + tree.occurrence)
      tree.sons.foreach(printTree(_, str + "\t"))
    }
    else {
      tree.sons.foreach(printTree(_, str))
    }
  }
}