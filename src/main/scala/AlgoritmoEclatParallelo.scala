import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.{ParMap, ParSeq}

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
  def prendiDatasetParallelo(): ParSeq[String] = {
    val filePath = "src/main/resources/dataset/datasetKaggleAlimenti.txt"
    val file = new File(filePath)
    Source.fromFile(file).getLines().toList.par //Contenuto di tutto il file come lista
  }

  //Parametro di Eclat
  val minSupport = 30

  //Contenuto File in Lista
  val scalaFileContentsList = prendiDatasetParallelo()

  def avvia(): Map[List[String], Int] = {
    //Lista degli alimenti presenti nel dataset (colonne) senza ripetizioni
    val listaAlimentiDS = scalaFileContentsList.mkString("").replaceAll("\n", ",").split(",").toList.distinct


    //Creo la lista di transazioni come matrice di zeri e uni
    val listaTransazioniIntermediaDS = for {
      list <- scalaFileContentsList
      alimento <- listaAlimentiDS
      presente = if (list.contains(alimento)) 1 else 0
    } yield presente


    //Formatto bene la lista a seconda di quanti alimenti univoci abbiamo
    val listaTransazioniDS = listaTransazioniIntermediaDS.seq.toList.grouped(listaAlimentiDS.size).toList.par

    /* Vettore con tutte le combinazioni di alimenti Es: Vector(List(Pane), List(Burro), List(Latte), List(CocaCola), List(Prosciutto), List(Pane, Burro), List(Pane, Latte), ...., List(Pane, Burro, Latte, CocaCola, Prosciutto))
     Senza ripetizioni */
    val combinazioniAlimenti = {
      for {
        iterazione <- 1 to (listaAlimentiDS.size)
        tupla <- listaAlimentiDS.combinations(iterazione)
      } yield tupla
    }.toList

    //Restituisce l'indice delle transazioni nella quale è presente tale alimento o tupla di alimenti
    def getTransationIDs(tupleAlimenti: Seq[String], transazione: List[Int]): Boolean = {
      val transitionIDs = for {
        a <- tupleAlimenti
      } yield transazione(listaAlimentiDS.indexOf(a))

      //println(transitionIDs)
      val result = transitionIDs forall (x => {
        x == 1
      })
      result
    }

    //Lista degli alimenti (o tuple di alimenti) assieme alle transazioni dove essi appaiono senza che siano raggruppati
    val listaAlimentiDSTransazioniNonRagg = for {
      transazione <- listaTransazioniDS
      tupleAlim <- combinazioniAlimenti
      if (getTransationIDs(tupleAlim, transazione))
    } yield (tupleAlim, transazione)


    //Raggruppamento della listaAlimentiDSTransazioniNonRagg per ottenere tutte le transazioni riguardo una certa combinazione
    val listaAlimTransaz = listaAlimentiDSTransazioniNonRagg.toList.par.groupBy(_._1).filter(_._2.size >= minSupport).seq

    //Creiamo la lista delle tuple con il relativo numero di transazioni annesso
    val listaAlimTransazSupport = listaAlimTransaz.map({
      case (k, v) => k -> v.size
    })

    listaAlimTransazSupport
  }


  //Valutiamo il risultato
  val result = time(avvia())

  //Riordiniamo il risultato per visualizzarlo meglio sul file
  val resultOrdered = result.toSeq.sortBy(_._2)

  //Scriviamo il risultato nel file
  val writingFile = new File("src/main/resources/results/EclatParallelResult.txt")
  val bw = new BufferedWriter(new FileWriter(writingFile))
  for (row <- resultOrdered) {
    bw.write(row + "\n")
  }
  bw.close
}
