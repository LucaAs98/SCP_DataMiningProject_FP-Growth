package eclat

import scala.annotation.tailrec
import utils.Utils._

object EclatRDD{

  def exec(minSupport: Int, pathInput:String, master: String): (Map[Set[String], Int], Long, Float) = {
    val sc = getSparkContext("EclatRDD", master)
    //Prendiamo il dataset (vedi Utils per dettagli)
    val (lines, dimDataset) = getRDD(pathInput, sc)
    val dataset = lines.map(x => x.split(" ").toSet)
    val transazioniFile = dataset.zipWithIndex.map({ x => x._2 -> x._1 })

    /* Funzione nella quale avviene tutto il processo dell'Eclat. Dati gli item singoli e le transazioni associate ad essi
      * calcoliamo anche le transazioni per le coppie, le triple ecc.. Queste tuple sono calolate in base a quelle che
      * abbiamo già trovato. Esempio: se come item singoli ne abbiamo solo 3, le coppie saranno formate solo da combinazioni di
      * quei 3. Così via per le tuple di dimensione maggiore. */
    def avviaIntersezione(transazioniElementiSingoli: Set[(Set[String], Set[Long])]): Map[Set[String], Set[Long]] = {

      @tailrec
      def intersezione(livelloCombinazione: Int, transazioniTrovate: Map[Set[String], Set[Long]]): Map[Set[String], Set[Long]] = {

        /* Tuple già trovate di dimensione maggiore. Utile per trovare gli item singoli delle possibili combinazioni
         * con dimensione maggiore. */
        val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

        //Prendimao singolarmente gli item che possiamo comporre per controllare nuove intersezioni
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
          if (livelloCombinazione < transazioniElementiSingoli.size) {
            intersezione(livelloCombinazione + 1, transazioniUnite)
          } else transazioniUnite
        } else transazioniTrovate
      }

      intersezione(2, transazioniElementiSingoli.toMap)
    }

    /* Funzione creata per calcolare il tempo dell'esecuzione, restituisce il risultato che otteniamo dalla computazione in
       modo tale da salvarlo su file. */


    def avviaAlgoritmo(): Map[Set[String], Int] = {

      val itemTransazioni = transazioniFile.flatMap(x => x._2.map(y => y -> x._1)).groupByKey().filter(_._2.size >= minSupport).map(x => Set(x._1) -> x._2.toSet).collect().toSet

      avviaIntersezione(itemTransazioni) map (elem => elem._1 -> elem._2.size)
    }

    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }
}