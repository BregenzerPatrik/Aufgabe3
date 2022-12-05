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
  val session: SparkSession = SparkSession
    .builder
    .appName("Aufgabe3")
    .master("local[*]")
    .getOrCreate()
  val schemaAll: StructType = new StructType()
    .add("id", LongType, nullable = false)
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, nullable = false)
        .add("name", StringType, nullable = true)
        .add("org", StringType, nullable = true)))
    .add("title", StringType, nullable = true)
    .add("year", IntegerType, nullable = true)
    .add("n_citation", IntegerType, nullable = true)
    .add("page_start", StringType, nullable = true)
    .add("page_end", StringType, nullable = true)
    .add("doc_type", StringType, nullable = true)
    .add("publisher", StringType, nullable = true)
    .add("volume", StringType, nullable = true)
    .add("issue", StringType, nullable = true)
    .add("doi", StringType, nullable = true)
    .add("references", ArrayType(LongType), nullable = true)
  val schemaOnlyID: StructType = new StructType()
    .add("id", LongType, nullable = false)
  val schemaDistinctAuthors: StructType = new StructType()
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, nullable = false)))
  val schemaAuthorsAll: StructType = new StructType()
    .add("authors", ArrayType(
      new StructType()
        .add("id", LongType, nullable = false)
        .add("name", StringType, nullable = true)
        .add("org", StringType, nullable = true)))

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
    val dataframe = getDataFrame(schemaDistinctAuthors, isJSON)
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
    val articleDF = getDataFrame(schemaAll, isJSON = true)
    articleDF.write.parquet(parquetPath)
  }
}
