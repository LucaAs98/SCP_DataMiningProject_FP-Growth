import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec

object provaSpark2 {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "provaSpark2")
    val lines = sc.textFile("C:\\Spark/datasetKaggleAlimenti10.txt")
    val dataset = lines.map(x => x.split(",").toSet)
    val transazioniFile = dataset.zipWithIndex.map({ x => x._2 -> x._1 })

    //Valuta il tempo di un'espressione
    def time[R](block: => R) = {
      val t0 = System.nanoTime()
      val result = block // call-by-name
      val t1 = System.nanoTime()
      println("Tempo di esecuzione: " + (t1 - t0) / 1000000 + "ms")
      result
    }

    //Parametro di Eclat
    val minSupport = 30


    /* Funzione creata per calcolare il tempo dell'esecuzione, restituisce il risultato che otteniamo dalla computazione in
       modo tale da salvarlo su file. */
    def avvia(): RDD[(Set[String], Set[Long])] = {

      val listaAlimentiSingoliTransazioni = transazioniFile.flatMap(x => x._2.map(y => Set(y) -> Set(x._1))).reduceByKey(_ ++ _).filter(_._2.size >= minSupport)

      /* Funzione nella quale avviene tutto il processo dell'Eclat. Dati gli alimenti singoli e le transazioni associate ad essi
      * calcoliamo anche le transazioni per le coppie, le triple ecc.. Queste tuple sono calolate in base a quelle che
      * abbiamo già trovato. Esempio: se come alimenti singoli ne abbiamo solo 3, le coppie saranno formate solo da combinazioni di
      * quei 3. Così via per le tuple di dimensione maggiore. */
      def avviaIntersezione(transazioniElementiSingoli: RDD[(Set[String], Set[Long])]): RDD[(Set[String], Set[Long])] = {
        @tailrec
        def intersezione(livelloCombinazione: Int, transazioniTrovate: RDD[(Set[String], Set[Long])]): RDD[(Set[String], Set[Long])] = {

          val transazioniTrovateCollect = transazioniTrovate.collect().toMap

          /* Tuple già trovate di dimensione maggiore. Utile per trovare gli alimenti singoli delle possibili combinazioni
        * con dimensione maggiore. */
          val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

          //Prendimao singolarmente gli alimenti che possiamo comporre per controllare nuove intersezioni
          val alimentiDaCombinare = tupleDimensMaggiore.flatMap(x => x._1).distinct()

          //Creiamo le possibili combinazioni tra essi
          val possibiliCombinazioni = sc.parallelize(alimentiDaCombinare.collect().toSet.subsets(livelloCombinazione).toSeq)

          //Creiamo una mappa che ha come chiave una possibile combinazione e come valore tutti i suoi subset.
          val tuplePossibiliMap = possibiliCombinazioni.map(tupla => tupla -> tupla.subsets(livelloCombinazione - 1).toSet)

          /* Se tutti i subset della combinazione sono presenti nelle transaszioni trovate finora, allora la combinazione
           * è un possibile candidato. */
          val tuplePossibiliCandidate = tuplePossibiliMap.filter(_._2.forall(transazioniTrovateCollect.contains))

          //Facciamo l'intersezione delle transazioni di ogni subset.
          val nuoveTupleTransazioni = tuplePossibiliCandidate.map(x => x._1 -> x._2.foldLeft(transazioniTrovateCollect(x._2.head))((acc, tuple) => acc.intersect(transazioniTrovateCollect(tuple)))).filter(_._2.size >= minSupport)

          //Se non abbiamo nuove tuple restituiamo quello che abbiamo trovato finora
          if (!nuoveTupleTransazioni.isEmpty()) {

            //Uniamo le transazioni che abbiamo trovato finora con quelle nuove
            val transazioniUnite = transazioniTrovate ++ nuoveTupleTransazioni

            /* Se dobbiamo controllare l'esistenza di altre combinazioni facciamo la chiamata ricorsiva a questa stessa funzione.
             * Altrimenti restituiamo il risultato. */
            if (livelloCombinazione < listaAlimentiSingoliTransazioni.count()) {
              intersezione(livelloCombinazione + 1, transazioniUnite)
            } else transazioniUnite
          } else transazioniTrovate
        }
        intersezione(2, transazioniElementiSingoli)
      }
      avviaIntersezione(listaAlimentiSingoliTransazioni)
    }

    //Valutiamo il risultato
    val result = time(avvia())
    val numTransazioni = transazioniFile.count().toFloat
    //Riordiniamo il risultato per visualizzarlo meglio sul file
    val resultOrdered = result.collect().toSeq.sortBy(_._2.size).map({
      case (k, v) => (k, (v.size, v.size.toFloat / numTransazioni))
    })

    //Scriviamo il risultato nel file
    val writingFile = new File("src/main/resources/results/EclatSparkResult10.txt")
    val bw = new BufferedWriter(new FileWriter(writingFile))
    for (row <- resultOrdered) {
      bw.write(row + "\n")
    }
    bw.close()
  }
}
