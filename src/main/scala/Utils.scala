import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object Utils {

  //Parametro di basket mining
  val minSupport = 30

  //Valuta il tempo di un'espressione
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Tempo di esecuzione: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  //Funzione per prendere il dataset dal file
  def prendiDataset(nomeFile: String): List[Set[String]] = {
    val filePath = "src/main/resources/dataset/" + nomeFile
    val file = new File(filePath)
    val source = Source.fromFile(file)
    val dataset = source.getLines().map(x => x.split(",").toSet).toList //Contenuto di tutto il file come lista
    source.close()
    dataset
  }

  //Calcolo delle confidenze
  def calcoloConfidenza(setItems: Set[String], numTransazioni: Float, listOfTables: Map[Set[String], Int]): Seq[String] = {
    val nTran = numTransazioni //numero di transazioni

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
      " confidence: " + (supportIS.toFloat / x._2))

    p
  }

  def scriviSuFileFrequentItemSet(result: Map[Set[String], Int], numTransazioni: Float, nomeFile: String): Unit = {
    //Riordiniamo il risultato per visualizzarlo meglio sul file
    val resultOrdered1 = result.toSeq.sortBy(_._2).map(elem => elem._1 -> (elem._2, elem._2.toFloat / numTransazioni)).toList.map(elem => elem.toString())

    scrivi(resultOrdered1, nomeFile)
  }

  def scriviSuFileSupporto(result: Map[Set[String], Int], numTransazioni: Float, nomeFile: String): Unit = {
    val result2 = result.filter(x => x._1.size > 1).keys.toList.flatMap(x => calcoloConfidenza(x, numTransazioni, result))

    scrivi(result2, nomeFile)
  }

  def scrivi(daScrivere: List[String], nomeFile: String): Unit = {
    val writingFile = new File("src/main/resources/results/" + nomeFile)
    val bw = new BufferedWriter(new FileWriter(writingFile))
    for (row <- daScrivere) {
      bw.write(row + "\n")
    }
    bw.close()
  }

  def getSparkContext(nomeContext: String): SparkContext = {
    val conf = new SparkConf().setAppName(nomeContext).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }

  def getRDD(nomeFile: String, sc: SparkContext): RDD[String] = {
    val file = "src/main/resources/dataset/" + nomeFile
    sc.textFile(file)
  }
}
