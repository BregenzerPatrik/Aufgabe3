import Spark.{gson, newJSONPath, oldJSONPath}

import java.io.{File, FileWriter}
import java.util.Scanner

object Main {
  def main(args: Array[String]): Unit = {
    Spark.session.sparkContext.setLogLevel("ERROR")
    //refactorJSON()
    val start = System.nanoTime()
    //Spark.writeParquet()
    //val articleCount = Spark.countArticles(isSQL = false,isJSON = false)
    val authorCount = Spark.distinctAuthors(false,false)
    //val mostArticlesDF = Spark.mostArticles(isSQL = true, isJSON = true)
    val end = System.nanoTime()
    val timeDifference = (end - start) / 1000000 //Umrechnung in ms
    println("Ausführungszeit: " + timeDifference + " ms")
    //println("Ermittelte Anzahl: "+articleCount)
    println("Einzigartige Autoren: "+authorCount)
    //println(mostArticlesDF.mkString(", "))
    System.in.read()
  }

  def refactorJSON(): Unit = {
    val file = new File(oldJSONPath)
    val file2 = new File(newJSONPath)
    val fileWriter = new FileWriter(newJSONPath)
    val scanner = new Scanner(file)
    scanner.nextLine()
    while (scanner.hasNextLine()) {
      val str = scanner.nextLine()

      if (str.startsWith(",")) {
        val line = gson.fromJson(str.substring(1), classOf[Line])
        fileWriter.write(gson.toJson(line) + "\n")
      }
      else if (!str.equals("]")) {
        val line = gson.fromJson(str, classOf[Line])
        fileWriter.write(gson.toJson(line) + "\n")
      }
    }
    scanner.close()
    fileWriter.close()
  }
}
