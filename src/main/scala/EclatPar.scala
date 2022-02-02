import scala.annotation.tailrec

/** Per commenti più dettagliati vedi file dell'algoritmo classico. * */
object EclatPar extends App {

  //Contenuto File in Lista
  val scalaFileContentsList = Utils.prendiDataset("datasetKaggleAlimenti.txt")
  val transazioniFile = scalaFileContentsList.zipWithIndex.map(elem => elem._2 -> elem._1)

  //Utile per il calcolo del supporto
  val numTransazioni = scalaFileContentsList.size


  /* Funzione creata per calcolare il tempo dell'esecuzione, restituisce il risultato che otteniamo dalla computazione in
     modo tale da salvarlo su file. */
  def avvia(): Map[Set[String], Set[Int]] = {

    //Lista degli alimenti presenti nel dataset (colonne) senza ripetizioni. //List("Pane", "Burro",....)
    val listaAlimentiDS = scalaFileContentsList.foldLeft(Set[String]())(_ ++ _)

    /* Il primo passo consiste nell'assegnare ad ogni alimento singolo l'ID delle transazioni in cui si trova.
    * Nel nostro caso l'ID è l'indice in cui la transazione si trova nel dataset. //(alimento, lista transazioni), (alimento, lista transazioni)...*/
    val primoPasso = listaAlimentiDS.map(alimento => (alimento, transazioniFile.filter(_._2.contains(alimento)).map(_._1)))

    /* Piccola ottimizzazione. Prendiamo gli alimenti singoli, sarà utile per creare le possibili combinazioni.  */
    val alimentiSingoliTransazioni = primoPasso.filter(_._2.size >= Utils.minSupport).map({ case (k, v) => Set(k) -> v.toSet })

    /* Funzione nella quale avviene tutto il processo dell'Eclat. Dati gli alimenti singoli e le transazioni associate ad essi
    * calcoliamo anche le transazioni per le coppie, le triple ecc.. Queste tuple sono calolate in base a quelle che
    * abbiamo già trovato. Esempio: se come alimenti singoli ne abbiamo solo 3, le coppie saranno formate solo da combinazioni di
    * quei 3. Così via per le tuple di dimensione maggiore. */
    def avviaIntersezione(transazioniElementiSingoli: Set[(Set[String], Set[Int])]): Map[Set[String], Set[Int]] = {

      @tailrec
      def intersezione(livelloCombinazione: Int, transazioniTrovate: Map[Set[String], Set[Int]]): Map[Set[String], Set[Int]] = {

        /* Tuple già trovate di dimensione maggiore. Utile per trovare gli alimenti singoli delle possibili combinazioni
        * con dimensione maggiore. */
        val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

        //Prendimao singolarmente gli alimenti che possiamo comporre per controllare nuove intersezioni
        val alimentiDaCombinare = tupleDimensMaggiore.foldLeft(Set[String]())((acc, elementoSingolo) => (elementoSingolo._1 ++ acc))

        //Creiamo le possibili combinazioni tra essi
        val possibiliCombinazioni = alimentiDaCombinare.subsets(livelloCombinazione).toSet

        //Creiamo una mappa che ha come chiave una possibile combinazione e come valore tutti i suoi subset.
        val tuplePossibiliMap = possibiliCombinazioni.map(tupla => tupla -> tupla.subsets(livelloCombinazione - 1).toSet)

        /* Se tutti i subset della combinazione sono presenti nelle transaszioni trovate finora, allora la combinazione
         * è un possibile candidato. */
        val tuplePossibiliCandidate = tuplePossibiliMap.filter(_._2.forall(transazioniTrovate.contains))

        //Facciamo l'intersezione delle transazioni di ogni subset.
        val nuoveTupleTransazioni = tuplePossibiliCandidate.par.map(x => x._1 -> x._2.foldLeft(transazioniTrovate(x._2.head))((acc, tuple) => acc.intersect(transazioniTrovate(tuple)))).filter(_._2.size >= Utils.minSupport)

        //Se non abbiamo nuove tuple restituiamo quello che abbiamo trovato finora
        if (nuoveTupleTransazioni.nonEmpty) {

          //Uniamo le transazioni che abbiamo trovato finora con quelle nuove
          val transazioniUnite = transazioniTrovate ++ nuoveTupleTransazioni

          /* Se dobbiamo controllare l'esistenza di altre combinazioni facciamo la chiamata ricorsiva a questa stessa funzione.
           * Altrimenti restituiamo il risultato. */
          if (livelloCombinazione < listaAlimentiDS.size) {
            intersezione(livelloCombinazione + 1, transazioniUnite)
          } else transazioniUnite
        } else transazioniTrovate
      }

      intersezione(2, transazioniElementiSingoli.toMap)
    }

    avviaIntersezione(alimentiSingoliTransazioni)
  }

  //Valutiamo il risultato
  val result = Utils.time(avvia())
  val result2 = result.map(elem => elem._1 -> elem._2.size)

  Utils.scriviSuFileFrequentItemSet(result2, numTransazioni.toFloat, "EclatResultParallelo.txt")
  Utils.scriviSuFileSupporto(result2, numTransazioni, "EclatResultSupportParallelo.txt")
}