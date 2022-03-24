name := "SCP_DataMiningProject_FP-Growth"

version := "0.1"

scalaVersion := "2.12.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3"
// spark ml
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"



def filterOut(name: String): Boolean = {

  val flag = name.contains("utils") || name.contains("classes")

  if(!flag)
    name.endsWith("RDD.class") || name.endsWith("RDD$.class") || name.endsWith("GCP.class") || name.endsWith("GCP$.class")
  else true
}

mappings in (Compile,packageBin) ~= {
  (ms: Seq[(File,String)]) =>
    ms filter { case (file, toPath) => filterOut(toPath) }
}