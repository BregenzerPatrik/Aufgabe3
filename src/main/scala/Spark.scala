import com.google.gson.Gson
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructType}

object Spark {
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
  val schemaDestinctAuthors = new StructType()
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, false)))
  val schemaAuthorsAll = new StructType()
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, false)
        .add("name", StringType, true)
        .add("org", StringType, true)))

  def mostArticles(isSQL: Boolean, isJSON: Boolean): List[Author] = {
    val dataFrame = getDataFrame(schemaAuthorsAll, isJSON)
    val authorsDataFrame = dataFrame.select(explode(col("authors"))).select("col.*")
    if (isSQL) {
      authorsDataFrame.createTempView("Authors")
      val distinctAuthors = session.sql(
        """SELECT id , name , org FROM Authors WHERE id in (SELECT distinct id FROM Authors
          |GROUP BY id
          |having count(id)=(SELECT MAX(article)
          |FROM (
          |SELECT id, COUNT(id) as article
          |FROM Authors
          |GROUP BY id)))""".stripMargin).dropDuplicates("id").collect()
      rowArray2authorList(distinctAuthors)
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

  def rowArray2IdList(rowArray: Array[Row]): List[Long] = {
    rowArray.toList.map(row => row.getLong(0))
  }

  def rowArray2authorList(rowArray: Array[Row]): List[Author] = {
    rowArray.toList.map(row => Author(row.getLong(0), row.getString(1), row.getString(2)))
  }

  def distinctAuthors(isSQL: Boolean, isJSON: Boolean): Long = {
    val dataframe = getDataFrame(schemaDestinctAuthors, isJSON)
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
}
