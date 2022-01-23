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
  val minSupport = 3

  //Contenuto File in Lista
  val scalaFileContentsList = prendiDataset()
  val transazioniFile = scalaFileContentsList.zipWithIndex.map({
    case (k, v) => (v, k)
  })
  val numTransazioni = scalaFileContentsList.size

  def avvia(): Map[List[String], List[Int]] = {

    //Lista degli alimenti presenti nel dataset (colonne) senza ripetizioni
    val listaAlimentiDS = scalaFileContentsList.mkString("").replaceAll("\n", ",").split(",").toList.distinct

    val primoPasso = for {
      transazione <- transazioniFile
      alimento <- listaAlimentiDS
      if (transazione._2.split(",").toList.contains(alimento))
    } yield (alimento, transazione._1)


    val transazioniElementiSingoli = primoPasso.groupBy(_._1).map({
      case (k, v) => (List(k), v.map(_._2))
    }) filter (_._2.size > minSupport)

    val keysAlimentiSingoli = transazioniElementiSingoli.keys

    @tailrec
    def intersezione(livelloCombinazione: Int, transazioni: Map[List[String], List[Int]]): Map[List[String], List[Int]] = {
      val listeAlimentiGrandi = transazioni.filter(_._1.size == livelloCombinazione - 1)
      val lista = for {
        elementoPiccolo <- keysAlimentiSingoli
        elementoGrande <- listeAlimentiGrandi.keys
        if (!elementoGrande.contains(elementoPiccolo.head))
        risultato = transazioniElementiSingoli(elementoPiccolo) intersect listeAlimentiGrandi(elementoGrande)
        if (risultato.size > minSupport)
      } yield ((elementoGrande appended (elementoPiccolo.head)), risultato)

      val listaOrdinata = lista.map({
        case (k: List[String], v) => (k.sortBy(x => x), v)
      })

      val merged = transazioni.toSeq ++ listaOrdinata.toSeq

      if (livelloCombinazione < listaAlimentiDS.size)
        intersezione(livelloCombinazione + 1, merged.toMap)
      else merged.toMap
    }

    intersezione(2, transazioniElementiSingoli)
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