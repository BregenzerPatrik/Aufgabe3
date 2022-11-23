import com.google.gson.Gson
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}

import java.io.{File, FileWriter}
import java.util.Scanner
import scala.annotation.tailrec

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
  val schemaDestinctAuthorsOnlyID = new StructType()
    .add("id", LongType, false)
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, false)))


  def main(args: Array[String]): Unit = {

    //refactorJSON()
    val start = System.nanoTime()
    //writeParquet()
    //val articleCount = countArticles(true,true)
    //val authorCount=distinctAuthors(true,false)
    val mostArticlesDF = mostArticles(true, true)
    val end = System.nanoTime()
    val timeDifference = (end - start) / 1000000 //Umrechnung in ms
    println("Ausführungszeit: " + timeDifference + " ms")
    println(mostArticlesDF.mkString(", "))
    //println("Ermittelte Anzahl: "+articleCount)
    //println("Einzigartige Autoren: "+authorCount)
    System.in.read()
  }

  def mostArticles(isSQL: Boolean, isJSON: Boolean): List[Author] = {
    val dataFrame = getDataFrame(schemaAll, isJSON)
    val authorsDataFrame = dataFrame.select(explode(col("authors"))).select("col.*")
    if (isSQL) {
      authorsDataFrame.createTempView("Authors")
      val authorIDs = session.sql(
        """SELECT id FROM Authors
          |GROUP BY id
          |having count(id)=(SELECT MAX(article)
          |FROM (
          |SELECT id, COUNT(id) as article
          |FROM Authors
          |GROUP BY id))""".stripMargin).collect()
      val authorIDsString = authorIDs.mkString(", ")
      val authorArray = session.sql("SELECT id , name , org FROM Authors WHERE id in (" + authorIDsString.substring(1, authorIDsString.length - 1) + ")").dropDuplicates("id").collect()
      rowArray2authorList(authorArray)
    } else {
      val groupedAuthorsDF = authorsDataFrame.groupBy(col("id")).count()
      val maxValue = groupedAuthorsDF.agg({
        "count" -> "max"
      }).collect()(0)
      val allIDs = groupedAuthorsDF.where("count==" + maxValue(0)).select(col("id")).collect()
      val allIDsLong = rowArray2IdList(allIDs)
      val filteredAuthors = authorsDataFrame.filter(row => allIDsLong.contains(row.getLong(0))).dropDuplicates("id").collect()
      rowArray2authorList(filteredAuthors)
    }
  }

  @tailrec
  def rowArray2IdList(rowArray: Array[Row], idList: List[Long] = List()): List[Long] = {
    if (rowArray.isEmpty) {
      return idList
    }
    val nextAuthor = rowArray.head
    val id = nextAuthor.getLong(0)
    rowArray2IdList(rowArray.filter(!_.equals(nextAuthor)), idList :+ id)
  }

  @tailrec
  def rowArray2authorList(rowArray: Array[Row], authorList: List[Author] = List()): List[Author] = {
    if (rowArray.isEmpty) {
      return authorList
    }
    val nextAuthor = rowArray.head
    val nextAuthorID = nextAuthor.getLong(0)
    val nextAuthorName = nextAuthor.getString(1)
    val nextAuthorOrg = nextAuthor.getString(2)
    rowArray2authorList(rowArray.filter(!_.equals(nextAuthor)), authorList :+ Author(nextAuthorID, nextAuthorName, nextAuthorOrg))
  }

  def distinctAuthors(isSQL: Boolean, isJSON: Boolean): Long = {
    val dataframe = getDataFrame(schemaDestinctAuthorsOnlyID, isJSON)
    if (isSQL) {
      val authorsDataFrame = dataframe.select(explode(col("authors")))
      authorsDataFrame.createTempView("AuthorIDs")
      session.sql("Select count(distinct *) FROM AuthorIDs").take(1)(0).getAs[Long](0)
    } else {
      val authorsDataFrame = dataframe.select(explode(col("authors")))
      authorsDataFrame.select(col("col")).distinct().count()
    }
  }

  def countArticles(isSQL: Boolean, isJSON: Boolean): Long = {
    val dataframe = getDataFrame(schemaOnlyID, isJSON)
    if (isSQL) {
      dataframe.createTempView("Article")
      session.sql(
        """
          |Select Count(*) from Article""".stripMargin).take(1)(0).getAs[Long](0)
    } else {
      dataframe.count()
    }


  }

  def getDataFrame(schema: StructType, isJSON: Boolean): sql.DataFrame = {
    if (isJSON) {
      session
        .read.schema(schema).json(newJSONPath)
    } else {
      session
        .read.schema(schema).parquet(parquetPath)
    }

  }

  def writeParquet(): Unit = {
    val articleDF = getDataFrame(schemaAll, true)
    articleDF.write.parquet(parquetPath)
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
