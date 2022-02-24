import scala.annotation.tailrec
import Utils._

/** Per commenti più dettagliati vedi file dell'algoritmo classico. * */
object EclatPar extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  val dataset = prendiDataset().par

  val transazioniFile = dataset.zipWithIndex.map(elem => elem._2 -> elem._1)

  //Utile per il calcolo del supporto
  val numTransazioni = dataset.size

  /* Funzione creata per calcolare il tempo dell'esecuzione, restituisce il risultato che otteniamo dalla computazione in
     modo tale da salvarlo su file. */
  def avvia(): Map[Set[String], Set[Int]] = {

    //Lista degli item presenti nel dataset (colonne) senza ripetizioni. //List("Pane", "Burro",....)
    val itemSingoli = dataset.foldLeft(Set[String]())(_ ++ _)

    /* Il primo passo consiste nell'assegnare ad ogni item singolo l'ID delle transazioni in cui si trova.
    * Nel nostro caso l'ID è l'indice in cui la transazione si trova nel dataset. //(item, lista transazioni), (item, lista transazioni)...*/
    val itemTransNotFiltered = itemSingoli.map(item => (item, transazioniFile.seq.filter(_._2.contains(item)).map(_._1)))

    /* Piccola ottimizzazione. Prendiamo gli item singoli, sarà utile per creare le possibili combinazioni.  */
    val itemTransazioni = itemTransNotFiltered.filter(_._2.size >= minSupport).map(elem => Set(elem._1) -> elem._2.toSet)

    /* Funzione nella quale avviene tutto il processo dell'Eclat. Dati gli item singoli e le transazioni associate ad essi
    * calcoliamo anche le transazioni per le coppie, le triple ecc.. Queste tuple sono calolate in base a quelle che
    * abbiamo già trovato. Esempio: se come item singoli ne abbiamo solo 3, le coppie saranno formate solo da combinazioni di
    * quei 3. Così via per le tuple di dimensione maggiore. */
    def avviaIntersezione(transazioniElementiSingoli: Set[(Set[String], Set[Int])]): Map[Set[String], Set[Int]] = {

      @tailrec
      def intersezione(livelloCombinazione: Int, transazioniTrovate: Map[Set[String], Set[Int]]): Map[Set[String], Set[Int]] = {

        /* Tuple già trovate di dimensione maggiore. Utile per trovare gli item singoli delle possibili combinazioni
        * con dimensione maggiore. */
        val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

        //Prendiamo singolarmente gli item che possiamo comporre per controllare nuove intersezioni
        val itemDaCombinare = tupleDimensMaggiore.foldLeft(Set[String]())((acc, elementoSingolo) => (elementoSingolo._1 ++ acc))

        //Creiamo le possibili combinazioni tra essi
        val possibiliCombinazioni = itemDaCombinare.subsets(livelloCombinazione).toSet

        //Creiamo una mappa che ha come chiave una possibile combinazione e come valore tutti i suoi subset.
        val tuplePossibiliMap = possibiliCombinazioni.map(tupla => tupla -> tupla.subsets(livelloCombinazione - 1).toSet)

        /* Se tutti i subset della combinazione sono presenti nelle transaszioni trovate finora, allora la combinazione
         * è un possibile candidato. */
        val tuplePossibiliCandidate = tuplePossibiliMap.filter(_._2.forall(transazioniTrovate.contains))

        //Facciamo l'intersezione delle transazioni di ogni subset.
        val nuoveTupleTransazioni = tuplePossibiliCandidate.par.map(x => x._1 -> x._2.foldLeft(transazioniTrovate(x._2.head))((acc, tuple) => acc.intersect(transazioniTrovate(tuple)))).filter(_._2.size >= minSupport)

        //Se non abbiamo nuove tuple restituiamo quello che abbiamo trovato finora
        if (nuoveTupleTransazioni.nonEmpty) {

          //Uniamo le transazioni che abbiamo trovato finora con quelle nuove
          val transazioniUnite = transazioniTrovate ++ nuoveTupleTransazioni

          /* Se dobbiamo controllare l'esistenza di altre combinazioni facciamo la chiamata ricorsiva a questa stessa funzione.
           * Altrimenti restituiamo il risultato. */
          if (livelloCombinazione < itemSingoli.size) {
            intersezione(livelloCombinazione + 1, transazioniUnite)
          } else transazioniUnite
        } else transazioniTrovate
      }

      intersezione(2, transazioniElementiSingoli.toMap)
    }

    avviaIntersezione(itemTransazioni)
  }

  //Valutiamo il risultato
  val result = time(avvia())
  val result2 = result.map(elem => elem._1 -> elem._2.size)

  scriviSuFileFrequentItemSet(result2, numTransazioni.toFloat, "EclatResultParallelo.txt")
  scriviSuFileSupporto(result2, numTransazioni, "EclatResultSupportParallelo.txt")
}