import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec
import scala.io.Source

object AlgoritmoEclat extends App {

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
  })

  //Utile per il calcolo del supporto
  val numTransazioni = scalaFileContentsList.size

  /* Funzione creata per calcolare il tempo dell'esecuzione, restituisce il risultato che otteniamo dalla computazione in
     modo tale da salvarlo su file. */
  def avvia(): Map[List[String], List[Int]] = {

    //Lista degli alimenti presenti nel dataset (colonne) senza ripetizioni. //List("Pane", "Burro",....)
    val listaAlimentiDS = scalaFileContentsList.mkString("").replaceAll("\n", ",").split(",").toList.distinct

    /* Il primo passo consiste nell'assegnare ad ogni alimento singolo l'ID delle transazioni in cui si trova.
    * Nel nostro caso l'ID è l'indice in cui la transazione si trova nel dataset. //(alimento, transazione), (alimento, transazione)...*/
    val primoPasso = for {
      transazione <- transazioniFile //Scorro le transazioni
      alimento <- listaAlimentiDS //Scorro i singoli alimenti
      if (transazione._2.split(",").toList.contains(alimento)) //Se l'alimento è contenuto nella transazione lo aggiungiamo
    } yield (alimento, transazione._1)

    /* Raggruppiamo per alimento in modo tale da avere per ciascuno di essi la lista delle transazioni in cui è contenuto. 
    * Inoltre eliminiamo gli alimenti che non rispettano il minimo supporto. */
    val transazioniElementiSingoli = primoPasso.groupBy(_._1).map({
      case (k, v) => (List(k), v.map(_._2))
    }) filter (_._2.size >= minSupport)

    /* Piccola ottimizzazione. Prendiamo gli alimenti singoli, sarà utile per non doverlo calcolare ogni volta 
    *  all'interno di "intersezione" */
    val alimentiSingoli = transazioniElementiSingoli.keys

    /* Funzione nella quale avviene tutto il processo dell'Eclat. Dati gli alimenti singoli e le transazioni associate ad essi
    * calcoliamo anche le transazioni per le coppie, le triple ecc.. Queste tuple sono calolate in base a quelle che 
    * abbiamo già trovato. Esempio: se come alimenti singoli ne abbiamo solo 3, le coppie saranno formate solo da combinazioni di 
    * quei 3. Così via per le tuple di dimensione maggiore. */
    def avviaIntersezione(transazioniElementiSingoli: Map[List[String], List[Int]]): Map[List[String], List[Int]] = {
      @tailrec
      def intersezione(livelloCombinazione: Int, transazioniTrovate: Map[List[String], List[Int]]): Map[List[String], List[Int]] = {

        /* Tuple già trovate di dimensione maggiore. Utile per calcolare le tuple di dimensione + 1 rispetto ad esse.
        * Infatti quest'ultime saranno calcolate grazie a tupleDimensMaggiore combinate con alimentiSingoli. */
        val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

        //Calcoliamo le transazioni per le tuple di dimensione maggiore rispetto a quelle già trovate //List((tupla, transazioni),....
        val tupleSuccessive = for {
          alimentoSingolo <- alimentiSingoli //Per ogni alimento singolo
          tuplaMassima <- tupleDimensMaggiore.keys //Per ogni tupla massima già trovata
          if (!tuplaMassima.contains(alimentoSingolo.head)) //Se l'elemento singolo non è si trova nella tuplaMassima

          //Facciamo l'intersezione tra i due per ottenere le transazioni dove appaiono entrambi
          risultato = transazioniElementiSingoli(alimentoSingolo) intersect tupleDimensMaggiore(tuplaMassima)
          if (risultato.size >= minSupport) //Se le transazioni in comune sono maggiori del minSupporto le aggiungiamo
        } yield ((tuplaMassima appended (alimentoSingolo.head)), risultato)

        //Le riordiniamo in ordine alfabetico in modo tale da rimuovere le combinazioni duplicate //(Pane, Burro), (Burro, Pane)
        val tupleSuccessiveOrdinate = tupleSuccessive.map({
          case (k: List[String], v) => (k.sortBy(x => x), v)
        })

        //Uniamo le transazioni che abbiamo trovato finora con quelle nuove
        val transazioniUnite = transazioniTrovate.toSeq ++ tupleSuccessiveOrdinate.toSeq

        /* Se dobbiamo controllare l'esistenza di altre combinazioni facciamo la chiamata ricorsiva a questa stessa funzione.
         * Altrimenti restituiamo il risultato. */
        if (livelloCombinazione < listaAlimentiDS.size)
          intersezione(livelloCombinazione + 1, transazioniUnite.toMap)
        else transazioniUnite.toMap
      }

      intersezione(2, transazioniElementiSingoli)
    }

    avviaIntersezione(transazioniElementiSingoli)
  }


  //Valutiamo il risultato
  val result = time(avvia())

  //Riordiniamo il risultato per visualizzarlo meglio sul file
  val resultOrdered = result.toSeq.sortBy(_._2.size).map({
    case (k, v) => (k, (v.size, v.size.toFloat / numTransazioni.toFloat))
  })

  //Scriviamo il risultato nel file
  val writingFile = new File("src/main/resources/results/EclatResult.txt")
  val bw = new BufferedWriter(new FileWriter(writingFile))
  for (row <- resultOrdered) {
    bw.write(row + "\n")
  }
  bw.close()
}