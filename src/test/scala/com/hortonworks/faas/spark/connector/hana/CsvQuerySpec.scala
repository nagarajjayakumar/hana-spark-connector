// scalastyle:off magic.number file.size.limit regex

package com.hortonworks.faas.spark.connector.hana

import org.apache.spark.sql.SQLContext
import org.scalatest.FlatSpec

/**
  * Read HanaDb tables as dataframes using the Spark DataSource API and
  * a SQL query to specify the desired data
  */
class CsvQuerySpec extends FlatSpec with SharedHanaDbContext{
  val carsFile = "src/test/resources/cars.csv"

  override def beforeAll(): Unit = {
    super.beforeAll()

    TestUtils.setupBasic(this)

  }

  "UserQuerySpec" should "create dataframe from user-specified query" in {

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .load(carsFile)

    //df.show()
    df.printSchema()
  }
}
