package mainClass

import apriori.{Apriori, AprioriPar, AprioriRDD}
import eclat.{Eclat, EclatPar, EclatRDD}
import fpgrowth.{FPGrowth, FPGrowthPar, FPGrowthRDD}
import fpgrowthmod.{FPGrowthMod, FPGrowthModPar, FPGrowthModRDD}
import fpgrowthstar.{FPGrowthStar, FPGrowthStarPar, FPGrowthStarRDD}
import nonord.{NonordFP, NonordFPPar, NonordFPRDD}
import demo._
import fpgrowthspark._
import utils.Utils.{scriviSuFileFrequentItemSet, scriviSuFileSupporto}

object MainClass {

  val algoritmo = 26
  val dataset = 0
  val flagScriviSuFile = false
  //Parametro di basket mining
  val minSupport = 30
  val numParts = 24
  val mappaAlgoritmi: Map[Int, String] = Map[Int, String](
    0 -> "Apriori",
    1 -> "AprioriPar",
    2 -> "AprioriRDD",
    3 -> "Eclat",
    4 -> "EclatPar",
    5 -> "EclatRDD",
    6 -> "FPGrowth",
    7 -> "FPGrowthPar",
    8 -> "FPGrowthRDD",
    9 -> "FPGrowthStar",
    10 -> "FPGrowthStarPar",
    11 -> "FPGrowthStarRDD",
    12 -> "NonordFP",
    13 -> "NonordFPPar",
    14 -> "NonordFPRDD",
    15 -> "FPGrowthMod",
    16 -> "FPGrowthModPar",
    17 -> "FPGrowthModRDD")

  val mappaNomiFile: Map[Int, String] = Map[Int, String](
    0 -> "datasetKaggleAlimenti.txt",
    1 -> "T10I4D100K.txt",
    2 -> "T40I10D100K.txt",
    3 -> "mushroom.txt",
    4 -> "connect.txt",
    5 -> "datasetLettereDemo.txt"
  )

  val nomeDataset: String = "src/main/resources/dataset/" + mappaNomiFile(dataset)

  def main(args: Array[String]): Unit = {

    val (result, time, size) = algoritmo match {
      case 0 => Apriori.exec(minSupport, nomeDataset)
      case 1 => AprioriPar.exec(minSupport, nomeDataset)
      case 2 => AprioriRDD.exec(minSupport, nomeDataset, "local[*]")
      case 3 => Eclat.exec(minSupport, nomeDataset)
      case 4 => EclatPar.exec(minSupport, nomeDataset)
      case 5 => EclatRDD.exec(minSupport, nomeDataset, "local[*]")
      case 6 => FPGrowth.exec(minSupport, nomeDataset)
      case 7 => FPGrowthPar.exec(minSupport, nomeDataset)
      case 8 => FPGrowthRDD.exec(minSupport, numParts, nomeDataset, "local[*]")
      case 9 => FPGrowthStar.exec(minSupport, nomeDataset)
      case 10 => FPGrowthStarPar.exec(minSupport, nomeDataset)
      case 11 => FPGrowthStarRDD.exec(minSupport, numParts, nomeDataset, "local[*]")
      case 12 => NonordFP.exec(minSupport, nomeDataset)
      case 13 => NonordFPPar.exec(minSupport, nomeDataset)
      case 14 => NonordFPRDD.exec(minSupport, numParts, nomeDataset, "local[*]")
      case 15 => FPGrowthMod.exec(minSupport, nomeDataset)
      case 16 => FPGrowthModPar.exec(minSupport, nomeDataset)
      case 17 => FPGrowthModRDD.exec(minSupport, numParts, nomeDataset, "local[*]")
      case 18 => AprioriDemo.exec(2, "src/main/resources/dataset/datasetLettereDemo.txt")
      case 19 => AprioriParDemo.exec(2000, "src/main/resources/dataset/T10I4D100K.txt")
      case 20 => EclatDemo.exec(2, "src/main/resources/dataset/datasetLettereDemo.txt")
      case 21 => FPGrowthDemo.exec(2, "src/main/resources/dataset/datasetLettereDemo.txt")
      case 22 => FPGrowthPar.exec(2,"src/main/resources/dataset/datasetLettereDemo.txt")
      case 23 => FPGrowthRDDDemo.exec(2, 4, "src/main/resources/dataset/datasetLettereDemo.txt", "local[*]")
      case 24 => FPGrowthStarDemo.exec(2, "src/main/resources/dataset/datasetLettereDemo.txt")
      case 25 => NonordFPDemo.exec(2, "src/main/resources/dataset/datasetLettereDemo.txt")
      case 26 => FPGrowthSpark.exec(minSupport, numParts, nomeDataset, "local[*]")}

    if (flagScriviSuFile && result.nonEmpty) {
      scriviSuFileFrequentItemSet(result, size, "src/main/resources/results/" + mappaAlgoritmi(algoritmo) + "Result.txt")
      scriviSuFileSupporto(result, size, "src/main/resources/results/" + mappaAlgoritmi(algoritmo) + "ConfidenzaResult.txt")
    }
  }
}