import org.apache.spark.mllib.fpm.FPGrowth
import Utils._
import java.io.{BufferedWriter, File, FileWriter}

//Versione FPGrowth spark già implementata
object FPGrowthSpark extends App {
  val sc = getSparkContext("FPGrowthSpark")
  //Prendiamo il dataset (vedi Utils per dettagli)
  val lines = getRDD(sc)
  val dataset = lines.map(x => x.split(spazioVirgola))

  //nostro supp/num. trans
  val fpg = new FPGrowth().setMinSupport(0.003).setNumPartitions(800)

  def avvia() = {
    val model = fpg.run(dataset)

    val freqItemSet = model.freqItemsets.collect().sortBy(x => x.freq)
    (model, freqItemSet)
  }

  val (model, result) = time(avvia())
  //Scriviamo il risultato nel file
  val writingFile = new File("src/main/resources/results/FPGrowthFreqItemSetSpark.txt")
  val bw = new BufferedWriter(new FileWriter(writingFile))
  for (row <- result) {
    bw.write(row.items.mkString("[", ",", "]" + "\tFrequenza: " + row.freq + "\n"))
  }
  bw.close()

  val minConfidence = 0.0
  val associationRules = model.generateAssociationRules(minConfidence).collect()

  //Scriviamo il risultato nel file
  val writingFile2 = new File("src/main/resources/results/FPGrowthAssRulestSpark.txt")
  val bw2 = new BufferedWriter(new FileWriter(writingFile2))
  for (row <- associationRules) {
    bw2.write("Antecedente " + row.antecedent.mkString("[", ",", "]\t=>" + "\tSuccessivo: " + row.consequent.mkString("[", ",", "]") + "\n"))
  }
  bw2.close()
}
