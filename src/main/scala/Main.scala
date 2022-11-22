import com.google.gson.Gson
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}

import java.io.{File, FileWriter}
import java.util.Scanner

object Main {
  val oldJSONPath = "../../dblp.v12.json"
  val newJSONPath = "../../dblp.v12.txt"
  val parquetPath = "../../Data.parquet"
  val gson = new Gson()
  val session = SparkSession
    .builder
    .appName("Aufgabe3")
    .master("local[*]")
    .getOrCreate()
  val schemaAll = new StructType()
    .add("id", LongType, false)
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, false)
        .add("name", StringType, true)
        .add("org", StringType, true)))
    .add("title", StringType, true)
    .add("year", IntegerType, true)
    .add("n_citation", IntegerType, true)
    .add("page_strart", StringType, true)
    .add("page_end", StringType, true)
    .add("doc_type", StringType, true)
    .add("publisher", StringType, true)
    .add("volume", StringType, true)
    .add("issue", StringType, true)
    .add("doi", StringType, true)
    .add("references", ArrayType(LongType), true)
  val schemaOnlyID = new StructType()
    .add("id", LongType, false)


  def main(args: Array[String]): Unit = {
    //refactorJSON()
    val start= System.nanoTime()
    //writeParquet()
    val count = countArticles(true,true)
    val end = System.nanoTime()
    val timeDifference= (end - start)/ 1000000 //Umrechnung in ms
    println("Ausführungszeit: "+timeDifference+" ms")
    println("Ermittelte Anzahl: "+count)
    System.in.read()
  }

  def countArticles(isSQL:Boolean,isJSON:Boolean): Long ={
    val dataframe=getDataFrame(schemaOnlyID,isJSON)
    if(isSQL){
      dataframe.createTempView("Article")
      session.sql(
        """
          |Select Count(*) from Article""".stripMargin).take(1)(0).getAs[Long](0)
    }else{
      dataframe.count()
    }


  }
  def getDataFrame(schema: StructType, isJSON:Boolean):sql.DataFrame ={
    if(isJSON){
      session
        .read.schema(schema).json(newJSONPath)
    }else{
      session
        .read.schema(schema).parquet(parquetPath)
    }

  }
  def writeParquet(): Unit ={
    val articleDF = getDataFrame(schemaAll,true)
    articleDF.write.parquet(parquetPath)
  }

  def refactorJSON(): Unit ={
    val file = new File(oldJSONPath)
    val file2 = new File(newJSONPath)
    val fileWriter = new FileWriter(newJSONPath)
    val scanner = new Scanner(file)
    scanner.nextLine()
    while (scanner.hasNextLine()){
      val str =  scanner.nextLine()

      if(str.startsWith(",")){
        val line = gson.fromJson(str.substring(1),classOf[Line])
        fileWriter.write(gson.toJson(line) +"\n")
      }
      else if(!str.equals("]")){
        val line = gson.fromJson(str,classOf[Line])
        fileWriter.write(gson.toJson(line) +"\n")
      }
    }
    scanner.close()
    fileWriter.close()

  }
}
