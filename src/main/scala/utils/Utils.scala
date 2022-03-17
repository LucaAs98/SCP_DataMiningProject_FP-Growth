package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object Utils {
  //Funzione per prendere il dataset dal file
  def prendiDataset(path: String): (List[Set[String]], Float) = {
    val file = new File(path)
    val source = Source.fromFile(file)
    val dataset = source.getLines().map(x => x.split(" ").toSet).toList //Contenuto di tutto il file come lista
    source.close()
    (dataset, dataset.size.toFloat)
  }

  //Valuta il tempo di un'espressione
  def time[R](block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val tempo = (t1 - t0) / 1000000
    println("Tempo di esecuzione: " + tempo + "ms")
    (result, tempo)
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
  def scriviSuFileFrequentItemSet(result: Map[Set[String], Int], numTransazioni: Float, path: String): Unit = {
    //Riordiniamo il risultato per visualizzarlo meglio sul file
    val resultOrdered1 = result.toSeq.sortBy(_._2).map(elem => elem._1 -> (elem._2, elem._2.toFloat / numTransazioni)).toList.map(elem => elem.toString())
    scrivi(resultOrdered1, path)
  }

  //Formatta il risultato ottenuto dalle computazioni in modo tale da calcolarne il supporto e lo salva su file
  def scriviSuFileSupporto(result: Map[Set[String], Int], numTransazioni: Float, path: String): Unit = {
    val result2 = result.filter(x => x._1.size > 1).keys.toList.flatMap(x => calcoloConfidenza(x, numTransazioni, result))

    scrivi(result2, path)
  }

  //Scrittura effettiva su file
  def scrivi(daScrivere: List[String], path: String): Unit = {
    val writingFile = new File(path)
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
  def getRDD(path: String, sc: SparkContext): (RDD[String], Float) = {
    val dataset = sc.textFile(path)
    (dataset, dataset.count().toFloat)
  }

  //Formatta il risultato ottenuto dalle computazioni in modo tale da calcolarne i frequentItemSet e lo salva su file
  def prova(result: Map[Set[String], Int], numTransazioni: Float): List[String] = {
    //Riordiniamo il risultato per visualizzarlo meglio sul file
    result.toSeq.map(elem => elem._1 -> (elem._2, elem._2.toFloat / numTransazioni)).toList.map(elem => elem.toString())
  }
}
