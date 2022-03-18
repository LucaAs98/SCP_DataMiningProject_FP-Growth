package mainClass

import apriori.AprioriRDD
import eclat.EclatRDD
import fpgrowth.FPGrowthRDD
import fpgrowthmod.FPGrowthModRDD
import fpgrowthstar.FPGrowthStarRDD
import nonord.NonordFPRDD
import org.apache.spark.SparkContext
import utils.Utils.formattaPerOutputGCP_FreItems

object MainClassGCP {
  val mappaNomiFile: Map[Int, String] = Map[Int, String](
    0 -> "datasetKaggleAlimenti.txt",
    1 -> "T10I4D100K.txt",
    2 -> "T40I10D100K.txt",
    3 -> "mushroom.txt",
    4 -> "connect.txt",
    5 -> "datasetLettereDemo.txt"
  )

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      throw new IllegalArgumentException(
        "Exactly 5 arguments are required: <inputPath> <outputPath> <algoritmo> <dataset> <minSupporto>")
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val algoritmo = args(2)
    val dataset = args(3).toInt
    val minSupport = args(4).toInt
    val numParts = if (args.length > 5) args(5).toInt else 100
    val nomeDataset = inputPath + mappaNomiFile(dataset)

    val (result, time, size) = algoritmo match {
      case "AprioriRDD" => AprioriRDD.exec(minSupport, nomeDataset)
      case "EclatRDD" => EclatRDD.exec(minSupport, nomeDataset)
      case "FPGrowthRDD" => FPGrowthRDD.exec(minSupport, numParts, nomeDataset)
      case "FPGrowthStarRDD" => FPGrowthStarRDD.exec(minSupport, numParts, nomeDataset)
      case "NonordFPRDD" => NonordFPRDD.exec(minSupport, numParts, nomeDataset)
      case "FPGrowthModRDD" => FPGrowthModRDD.exec(minSupport, numParts, nomeDataset)
    }

    /*** DA SISTEMARE SPARK CONTEXT ***/
    val sparkContext = SparkContext.getOrCreate()
    sparkContext.parallelize(formattaPerOutputGCP_FreItems(result, size)).saveAsTextFile(outputPath + "/" + algoritmo + "Result")
    //scriviSuFileSupporto(result, size, outputPath + "/" + algoritmo + "ConfidenzaResult")//.txt")
  }
}