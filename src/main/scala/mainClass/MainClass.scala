package mainClass

import apriori.{Apriori, AprioriPar, AprioriRDD}
import eclat.{Eclat, EclatPar, EclatRDD}
import fpgrowth.{FPGrowth, FPGrowthPar, FPGrowthRDD}
import fpgrowthmod.{FPGrowthMod, FPGrowthModPar, FPGrowthModRDD}
import fpgrowthstar.{FPGrowthStar, FPGrowthStarPar, FPGrowthStarRDD}
import nonord.{NonordFP, NonordFPPar, NonordFPRDD}
import utils.Utils.{scriviSuFileFrequentItemSet, scriviSuFileSupporto}

object MainClass {

  val algoritmo = 0
  val dataset = 0
  val flagScriviSuFile = true
  //Parametro di basket mining
  val minSupport = 2000

  val mappaAlgoritmi: Map[Int, String] = Map[Int, String](
    1 -> "Apriori",
    2 -> "AprioriPar",
    3 -> "AprioriRDD",
    4 -> "Eclat",
    5 -> "EclatPar",
    6 -> "EclatRDD",
    7 -> "FPGrowth",
    8 -> "FPGrowthPar",
    9 -> "FPGrowthRDD",
    10 -> "FPGrowthStar",
    11 -> "FPGrowthStarPar",
    12 -> "FPGrowthStarRDD",
    13 -> "NonordFP",
    14 -> "NonordFPPar",
    15 -> "NonordFPRDD",
    16 -> "FPGrowthMod",
    17 -> "FPGrowthModPar",
    18 -> "FPGrowthModRDD",
    19 -> "FPGrowthSpark")

  val mappaNomiFile: Map[Int, String] = Map[Int, String](
    0 -> "datasetKaggleAlimenti.txt",
    1 -> "T10I4D100K.txt",
    2 -> "T40I10D100K.txt",
    3 -> "mushroom.txt",
    4 -> "connect.txt",
    5 -> "datasetLettereDemo.txt"
  )

  def nomeFile: String = mappaNomiFile(dataset)

  def main(args: Array[String]) {

    /*
    if (args.length != 2) {
      throw new IllegalArgumentException(
        "Exactly 2 arguments are required: <inputPath> <outputPath>")
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val algortmo
    val support
    val dataset
    val partizioni
    */


    val (result, time) = algoritmo match {
      case 0 => Apriori.exec()
      case 1 => AprioriPar.exec()
      case 2 => AprioriRDD.exec()
      case 3 => Eclat.exec()
      case 4 => EclatPar.exec()
      case 5 => EclatRDD.exec()
      case 6 => FPGrowth.exec()
      case 7 => FPGrowthPar.exec()
      case 8 => FPGrowthRDD.exec()
      case 9 => FPGrowthStar.exec()
      case 10 => FPGrowthStarPar.exec()
      case 11 => FPGrowthStarRDD.exec()
      case 12 => NonordFP.exec()
      case 13 => NonordFPPar.exec()
      case 14 => NonordFPRDD.exec()
      case 15 => FPGrowthMod.exec()
      case 16 => FPGrowthModPar.exec()
      case 17 => FPGrowthModRDD.exec()
    }


    if (flagScriviSuFile) {
      scriviSuFileFrequentItemSet(result, dataset.size.toFloat, mappaAlgoritmi(algoritmo) + "Result.txt")
      scriviSuFileSupporto(result, dataset.size.toFloat, mappaAlgoritmi(algoritmo) + "ConfidenzaResult.txt")
    }

  }
}
