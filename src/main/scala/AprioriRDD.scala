import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

object AprioriRDD extends App {
  val conf = new SparkConf().setAppName("AprioriRDD").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val file = "src/main/resources/dataset/datasetKaggleAlimenti.txt"
  val dataset = sc.textFile(file)
  val min_sup = 30

  //Valuta il tempo di un'espressione
  def time[R](block: => R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Tempo di esecuzione: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  def gen(itemSets: RDD[Set[String]], size: Int): List[Set[String]] = {
    (itemSets reduce ((x, y) => x ++ y)).subsets(size).toList
  }

  def prune(itemCandRDD: RDD[Set[String]], itemsSetPrec: Array[Set[String]]) = {
    (itemCandRDD filter (x => x.subsets(x.size - 1) forall (y => itemsSetPrec.contains(y)))).collect()
  }

  def listOfPairs(str: String, itemSets: Array[Set[String]], size: Int) = {
    val items = str.split(",").toSet
    if (items.size >= size) {
      (itemSets.filter(y => y.subsetOf(items)))
    }
    else {
      Array[Set[String]]()
    }
  }
  //dafinire
  def countItemSet(itemSets: Array[Set[String]], size: Int) = {
    val ciao = dataset.flatMap(x => listOfPairs(x, itemSets, size))
    val itemPairs = ciao.map(x => (x, 1))
    val itemCounts = itemPairs.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= min_sup)
    itemCounts.collect().toMap
  }

  @tailrec
  def aprioriIter(mapItem: Map[Set[String], Int], dim: Int):Map[Set[String], Int] = {
    val itemsSetPrec = sc.parallelize(mapItem.keys.filter(x => x.size == (dim - 1)).toList) //vedere se cambia qualcosa con persist o meno
    val candidati = gen(itemsSetPrec, dim)
    val itemSets = prune(sc.parallelize(candidati), itemsSetPrec.collect())
    val itemSetCounts= time(countItemSet(itemSets, dim).filter(_._2 >= min_sup))

    if(itemSetCounts.isEmpty){
      mapItem
    }
    else
      {
        aprioriIter(mapItem ++ itemSetCounts, dim+1)
      }
  }

  def exec(): Unit = {
    val items = dataset.flatMap(x => x.split(","))
    val itemPairs = items.map(x => (Set(x), 1))
    val itemCounts = itemPairs.reduceByKey((v1, v2) => v1 + v2).filter(_._2 >= min_sup)
    val k = itemCounts.collect().toMap

    if (k.isEmpty) {
      k
    }
    else {
      val p = aprioriIter(k, 2)
      //p.toList.sortBy(_._2).foreach(println(_))
      //println(p.size)
    }
  }

  time(exec())

  val i = 0
  while(i < 1000000000000L){

  }
}
