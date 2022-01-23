import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec
import scala.io.Source
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.{ParMap, ParSeq}

/** * Per commenti più precisi vedi il file con l'algoritmo senza parallelizzazione ** */
object AlgoritmoEclatParallelo extends App {

  //Valuta il tempo di un'espressione
  def time[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Tempo di esecuzione: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  //Funzione per prendere il dataset dal file
  def prendiDataset(): List[String] = {
    val filePath = "src/main/resources/dataset/datasetKaggleAlimenti.txt"
    val file = new File(filePath)
    Source.fromFile(file).getLines().toList //Contenuto di tutto il file come lista
  }

  //Parametro di Eclat
  val minSupport = 30

  //Contenuto File in Lista
  val scalaFileContentsList = prendiDataset()
  val transazioniFile = scalaFileContentsList.zipWithIndex.map({
    case (k, v) => (v, k)
  }).par

  //Utile per il calcolo del supporto
  val numTransazioni = scalaFileContentsList.size

  def avvia(): ParMap[List[String], List[Int]] = {

    //Lista degli alimenti presenti nel dataset (colonne) senza ripetizioni
    val listaAlimentiDS = scalaFileContentsList.mkString("").replaceAll("\n", ",").split(",").toList.distinct

    //Lista con (alimento, transazione) per ogni alimento e per ogni transazione in cui si trova
    val primoPasso = for {
      transazione <- transazioniFile
      alimento <- listaAlimentiDS
      if (transazione._2.split(",").toList.contains(alimento))
    } yield (alimento, transazione._1)

    //Crea la lista di alimenti singoli con ogni transazione in cui si trovano
    val transazioniElementiSingoli = primoPasso.groupBy(_._1).map({
      case (k, v) => (List(k), v.map(_._2).toList)
    }) filter (_._2.size >= minSupport)

    //Piccola ottimizzazione per non calcolarlo ogni volta nel for
    val alimentiSingoli = transazioniElementiSingoli.keys

    //Funzione che computa ogni passo successivo, aumentando gradualmente la dimensione delle tuple da controllare
    def avviaIntersezione(transazioniElementiSingoli: ParMap[List[String], List[Int]]): ParMap[List[String], List[Int]] = {

      @tailrec
      def intersezione(livelloCombinazione: Int, transazioniTrovate: ParMap[List[String], List[Int]]): ParMap[List[String], List[Int]] = {
        //Tuple già trovate di dimensione maggiore
        val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

        //Calcolo delle tuple con dimensione maggiore rispetto a quelle già trovate
        val tupleSuccessive = for {
          alimentoSingolo <- alimentiSingoli
          tuplaMassima <- tupleDimensMaggiore.keys
          if (!tuplaMassima.contains(alimentoSingolo.head))

          risultato = transazioniElementiSingoli(alimentoSingolo) intersect tupleDimensMaggiore(tuplaMassima)
          if (risultato.size >= minSupport)
        } yield ((tuplaMassima appended (alimentoSingolo.head)), risultato)

        //Le riordiniamo in ordine alfabetico in modo tale da rimuovere le combinazioni duplicate //(Pane, Burro), (Burro, Pane)
        val tupleSuccessiveOrdinate = tupleSuccessive.map({
          case (k, v) => (k.sortBy(x => x), v)
        })

        //Uniamo le transazioni che abbiamo trovato finora con quelle nuove
        val transazioniUnite = transazioniTrovate.toSet union tupleSuccessiveOrdinate.toMap.toSet

        /* Se dobbiamo controllare l'esistenza di altre combinazioni facciamo la chiamata ricorsiva a questa stessa funzione.
         * Altrimenti restituiamo il risultato. */
        if (livelloCombinazione < listaAlimentiDS.size)
          intersezione(livelloCombinazione + 1, transazioniUnite.par.toMap)
        else transazioniUnite.toMap
      }

      intersezione(2, transazioniElementiSingoli)
    }

    avviaIntersezione(transazioniElementiSingoli)
  }

  //Valutiamo il risultato
  val result = time(avvia())

  //Riordiniamo il risultato per visualizzarlo meglio sul file
  val resultOrdered = result.seq.toSeq.sortBy(_._2.size).map({
    case (k, v) => (k, (v.size, v.size.toFloat / numTransazioni.toFloat))
  })

  //Scriviamo il risultato nel file
  val writingFile = new File("src/main/resources/results/EclatResultParallelo.txt")
  val bw = new BufferedWriter(new FileWriter(writingFile))
  for (row <- resultOrdered) {
    bw.write(row + "\n")
  }
  bw.close()
}