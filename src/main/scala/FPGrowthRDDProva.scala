import java.{util => ju}

import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec

object FPGrowthRDDProva extends App {
  val sc = Utils.getSparkContext("FpGrowthRDD")
  val lines = Utils.getRDD("datasetKaggleAlimenti.txt", sc)
  val dataset = lines.map(x => x.split(","))
  val dataset2 = sc.parallelize(
    List(Array("a", "c", "d", "f", "g", "i", "m", "p")
      , Array("a", "b", "c", "f", "i", "m", "o")
      , Array("b", "f", "h", "j", "o")
      , Array("b", "c", "k", "s", "p")
      , Array("a", "c", "e", "f", "l", "m", "n", "p")))


  val numParts = 10

  def getSingleItemCount(partitioner: Partitioner): Array[(String, Int)] = {
    dataset2.flatMap(t => t).map(v => (v, 1))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= Utils.minSupport)
      .collect()
      .sortBy(x => (-x._2, x._1))
  }

  //Aggiungiamo un nodo di una transazione all'albero
  @tailrec
  def addNodeTransaction(lastNode: Node[Int], transazione: Array[Int]): Unit = {
    //Passo base
    if (transazione.nonEmpty) {
      //Aggiungiamo all'ultimo nodo creato il nuovo
      val node = lastNode.add(transazione.head)
      //Se il nodo era già presente continuiamo l'aggiunta degli elementi senza aggiornare l'ht
      addNodeTransaction(node._1, transazione.tail)
    }
  }


  def addTrans(nodo: Node[Int], item: Map[String, Int], trans: Array[String]) = {

    //Ordiniamo la transazione
    val filtered = trans.flatMap(item.get)
    ju.Arrays.sort(filtered)
    addNodeTransaction(nodo, filtered)
    nodo
  }

  def printTree(tree: Node[Int], items: Map[Int, String], str: String): Unit = {
    if (tree.occurrence != -1) {
      println(str + items(tree.value) + " " + tree.occurrence)
      tree.sons.foreach(printTree(_, items, str + "\t"))
    }
    else {
      tree.sons.foreach(printTree(_, items, str))
    }
  }


  def exec(): Unit = {

    val partitioner = new HashPartitioner(numParts)

    //First step
    val singleItemsCount = getSingleItemCount(partitioner)
    val itemToRank = singleItemsCount.map(_._1).zipWithIndex.toMap

    //itemToRank.toList.sortBy(_._2).foreach(println)
    //Dataset ordinato con albero, o forse solo albero

    val caccaNodo = dataset2.aggregate(new Node(-1, List[Node[Int]]()))(((node,trans) => addTrans(node,itemToRank, trans)),
      ((node1, node2) => node1.merge(node2)))


      //transaction => addTrans(new Node[Int](-1, List[Node[Int]]()), itemToRank, transaction))
   /* addTrans(firstNode, itemToRank, dataset2.first())
    addTrans(firstNode, itemToRank, dataset2.first())
    addTrans(firstNode, itemToRank, dataset2.collect().toList(1))
    addTrans(firstNode, itemToRank, dataset2.collect().toList(4))*/
    val indexToitem = itemToRank.map(x => x._2 -> x._1)

    printTree(caccaNodo, indexToitem, "")


  }

  Utils.time(exec())

}
