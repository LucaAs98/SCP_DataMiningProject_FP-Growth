package eclat

import fpgrowthold.FPGrowthModOld.{avviaAlgoritmo, dimDataset}
import utils.Utils._

import scala.annotation.tailrec
import mainClass.MainClass.minSupport

object Eclat extends App {
  //Prendiamo il dataset (vedi Utils per dettagli)
  //val dataset = prendiDataset()
  val (dataset, dimDataset) = prendiDataset()

  //Creiamo gli ID per ogni transazione dato che non sono presenti nel dataset
  val transazioniFile = dataset.zipWithIndex.map({ x => (x._2, x._1) })

  //Utile per il calcolo del supporto
  val numTransazioni = dataset.size

  /* Primo passo nella quale restituiamo il set di item con le transazioni associate e gli item singoli. */
  def firstStep() = {
    val itemSingoli = dataset.foldLeft(Set[String]())(_ ++ _)
    val itemTransazioni = itemSingoli.map(item => (item, transazioniFile.filter(_._2.contains(item)).map(_._1)))
    (itemTransazioni, itemSingoli)
  }

  /* Funzione nella quale avviene tutto il processo dell'Eclat. Dati gli item singoli e le transazioni associate ad essi
     * calcoliamo anche le transazioni per le coppie, le triple ecc.. Queste tuple sono calolate in base a quelle che
     * abbiamo già trovato. Esempio: se come item singoli ne abbiamo solo 3, le coppie saranno formate solo da combinazioni di
     * quei 3. Così via per le tuple di dimensione maggiore. */
  def avviaIntersezione(transazioniElementiSingoli: Set[(Set[String], Set[Int])], itemSingoli: Set[String]): Map[Set[String], Set[Int]] = {

    @tailrec
    def intersezione(livelloCombinazione: Int, transazioniTrovate: Map[Set[String], Set[Int]]): Map[Set[String], Set[Int]] = {

      /* Tuple già trovate di dimensione maggiore. Utili per trovare con quali item dobbiamo creare le tuple di dimensione
      * "livelloCombinazione", dunque del passo che stiamo eseguendo. */
      val tupleDimensMaggiore = transazioniTrovate.filter(_._1.size == livelloCombinazione - 1)

      //Prendiamo singolarmente gli item che possiamo comporre per controllare nuove intersezioni
      val itemDaCombinare = tupleDimensMaggiore.foldLeft(Set[String]())((acc, elementoSingolo) => (elementoSingolo._1 ++ acc))

      //Creiamo le possibili combinazioni tra essi
      val possibiliCombinazioni = itemDaCombinare.subsets(livelloCombinazione).toSet

      /* Creiamo la mappa che avrà come chiave una possibile combinazione di dimensione "livelloCombinazione" e come valore tutti
       * i subset di dimensione "livelloCombinazione - 1" rispetto alla tupla. */
      val tuplePossibiliMap = possibiliCombinazioni.map(tupla => tupla -> tupla.subsets(livelloCombinazione - 1).toSet)

      /* Filtriamo i subset di (dimensione - 1) già presenti nelle transazioni che abbiamo trovato finora in modo tale da
       * eliminare in anticipo quelli non possibili */
      val tuplePossibiliCandidate = tuplePossibiliMap.filter(_._2.forall(transazioniTrovate.contains))

      /* Creiamo le nuove tuple di dimensione "livelloCombinazione" da appendere a quelle già trovate.
      * Prendiamo le candidate e, per ognuna di esse, creiamo la mappa composta da tupla (di dimensione "livelloCombinazione")
      * e le transazioni derivate dalle intersezioni di tutte le sottotuple da cui è composta. // tupla -> transazioni associate.
      * Usiamo un accumulatore che inizializziamo con le transazioni della prima sotto-tupla e calcoliamo le intersezioni
      * con le sotto-tuple successive che compongono la tupla grande. Filtriamo infine per il minSupport. */
      val nuoveTupleTransazioni = tuplePossibiliCandidate.map(x => x._1 -> x._2.foldLeft(transazioniTrovate(x._2.head))
      ((acc, tuple) => acc.intersect(transazioniTrovate(tuple)))).filter(_._2.size >= minSupport)

      //Se non ci sono più nuuove tuple abbiamo finito, altrimenti andiamo a calcolare quelle di dimensione maggiore.
      if (nuoveTupleTransazioni.nonEmpty) {

        //Uniamo le transazioni che abbiamo trovato finora con quelle nuove
        val transazioniUnite = transazioniTrovate ++ nuoveTupleTransazioni

        /* Se dobbiamo controllare l'esistenza di altre combinazioni facciamo la chiamata ricorsiva a questa stessa funzione.
         * Altrimenti restituiamo il risultato. */
        if (livelloCombinazione < itemSingoli.size) {
          //Chiamata ricorsiva
          intersezione(livelloCombinazione + 1, transazioniUnite)
        } else transazioniUnite
      } else transazioniTrovate
    }

    intersezione(2, transazioniElementiSingoli.toMap)
  }

  /* Funzione creata per calcolare il tempo dell'esecuzione, restituisce il risultato che otteniamo dalla computazione in
     modo tale da salvarlo su file. */
  /*def exec(): Map[Set[String], Int] = {

    /* Il primo passo consiste nell'assegnare ad ogni item singolo l'ID delle transazioni in cui si trova.
    * Nel nostro caso l'ID è l'indice in cui la transazione si trova nel dataset. //(item, lista transazioni), (item, lista transazioni)...*/
    val (itemTransNotFiltered, itemSingoli) = firstStep()

    /* Piccola ottimizzazione. Prendiamo gli item singoli (sarà utile per non doverli calcolare ogni volta
    * all'interno di "intersezione"), filtriamoli per minSupport e trasformiamo la chiave e la lista delle transazioni in set.
    * Ci servirà per la ricorsione. */
    val itemTransazioni = itemTransNotFiltered.filter(_._2.size >= minSupport).map(elem => Set(elem._1) -> elem._2.toSet)

    avviaIntersezione(itemTransazioni, itemSingoli).map(elem => elem._1 -> elem._2.size)
  }*/
  //Esecuzione effettiva dell'algoritmo
  def exec() = {
    val (result, tempo) = time(avviaAlgoritmo())
    (result, tempo, dimDataset)
  }
  def avviaAlgoritmo():Map[Set[String], Int] = {
    /* Il primo passo consiste nell'assegnare ad ogni item singolo l'ID delle transazioni in cui si trova.
    * Nel nostro caso l'ID è l'indice in cui la transazione si trova nel dataset. //(item, lista transazioni), (item, lista transazioni)...*/
    val (itemTransNotFiltered, itemSingoli) = firstStep()

    /* Piccola ottimizzazione. Prendiamo gli item singoli (sarà utile per non doverli calcolare ogni volta
    * all'interno di "intersezione"), filtriamoli per minSupport e trasformiamo la chiave e la lista delle transazioni in set.
    * Ci servirà per la ricorsione. */
    val itemTransazioni = itemTransNotFiltered.filter(_._2.size >= minSupport).map(elem => Set(elem._1) -> elem._2.toSet)

    avviaIntersezione(itemTransazioni, itemSingoli).map(elem => elem._1 -> elem._2.size)

  }

  //Valutiamo il risultato
 /* val result = time(exec())

  scriviSuFileFrequentItemSet(result, numTransazioni.toFloat, "EclatResult.txt")
  scriviSuFileSupporto(result, numTransazioni, "EclatResultSupport.txt")*/
}
