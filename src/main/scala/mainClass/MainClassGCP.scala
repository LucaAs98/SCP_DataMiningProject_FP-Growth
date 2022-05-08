package mainClass

import apriori.AprioriRDD
import eclat.EclatRDD
import fpgrowth.FPGrowthRDD
import fpgrowthmod.FPGrowthModRDD
import fpgrowthstar.FPGrowthStarRDD
import nonord.NonordFPRDD
import org.apache.spark.SparkContext
import utils.Utils.{formattaPerOutputGCP_FreItems, formattaPerOutputGCP_Supporto}

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
    val numParts = if (args.length > 5) args(5).toInt else 36
    val saveOutput = if (args.length > 6) args(6).toBoolean else true
    val nomeDataset = inputPath + mappaNomiFile(dataset)

    val (result, time, size) = algoritmo match {
      case "AprioriRDD" => AprioriRDD.exec(minSupport, nomeDataset, "yarn")
      case "EclatRDD" => EclatRDD.exec(minSupport, nomeDataset, "yarn")
      case "FPGrowthRDD" => FPGrowthRDD.exec(minSupport, numParts, nomeDataset, "yarn")
      case "FPGrowthStarRDD" => FPGrowthStarRDD.exec(minSupport, numParts, nomeDataset, "yarn")
      case "NonordFPRDD" => NonordFPRDD.exec(minSupport, numParts, nomeDataset, "yarn")
      case "FPGrowthModRDD" => FPGrowthModRDD.exec(minSupport, numParts, nomeDataset, "yarn")
    }

    if (saveOutput) {
      val sparkContext = SparkContext.getOrCreate()
      sparkContext.parallelize(formattaPerOutputGCP_FreItems(result, size)).saveAsTextFile(outputPath + "/" + algoritmo + "Result")
      sparkContext.parallelize(formattaPerOutputGCP_Supporto(result, size)).saveAsTextFile(outputPath + "/" + algoritmo + "ConfidenzaResult")
    }
  }
}