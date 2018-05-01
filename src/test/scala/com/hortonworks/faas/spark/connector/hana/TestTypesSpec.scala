// scalastyle:off magic.number file.size.limit regex

package com.hortonworks.faas.spark.connector.hana

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.scalatest.FlatSpec

case class HanaDbType(columnType: String, maybeColumnDefinition: Option[String] = None) {
  def columnName: String = s"t_$columnType"
  def columnDefinition: String = s"$columnName ${maybeColumnDefinition.getOrElse(columnType)}"
}

case class TypeMapping(sparkType: DataType, HanaDbType: HanaDbType, values: Seq[Any])

class TestTypesSpec extends FlatSpec with SharedHanaDbContext{
  def types: Seq[TypeMapping] = Seq(
    TypeMapping(ShortType, HanaDbType("TINYINT"), Seq(0.toShort, 16.toShort, null)),
    TypeMapping(ShortType, HanaDbType("SMALLINT"), Seq(0.toShort, 64.toShort, null)),

    TypeMapping(IntegerType, HanaDbType("MEDIUMINT"), Seq(0, 256, null)),
    TypeMapping(IntegerType, HanaDbType("INT"), Seq(0, 1024, null)),

    TypeMapping(LongType, HanaDbType("BIGINT"), Seq(0L, 2048L, null)),

    TypeMapping(DecimalType(38, 30), HanaDbType("DECIMAL", Some("DECIMAL(38, 30)")),
      Seq(new BigDecimal(0.0), new BigDecimal(1 / 3.0), null)),

    // FLOAT, REAL and DOUBLE "NULL" behavior is also covered in NullValueSpec
    TypeMapping(FloatType, HanaDbType("FLOAT"), Seq(0.0f, 1.5f, null)),

    TypeMapping(DoubleType, HanaDbType("REAL"), Seq(0.0, 1.6, null)),
    TypeMapping(DoubleType, HanaDbType("DOUBLE"), Seq(0.0, 1.9, null)),

    TypeMapping(StringType, HanaDbType("CHAR"), Seq("", "a", null)),
    TypeMapping(StringType, HanaDbType("VARCHAR", Some("VARCHAR(255)")), Seq("", "abc", null)),
    TypeMapping(StringType, HanaDbType("TINYTEXT"), Seq("", "def", null)),
    TypeMapping(StringType, HanaDbType("MEDIUMTEXT"), Seq("", "ghi", null)),
    TypeMapping(StringType, HanaDbType("LONGTEXT"), Seq("", "jkl", null)),
    TypeMapping(StringType, HanaDbType("TEXT"), Seq("", "mno", null)),
    TypeMapping(StringType, HanaDbType("ENUM", Some("ENUM('a', 'b', 'c')")), Seq("", "a", null)),
    TypeMapping(StringType, HanaDbType("SET", Some("SET('d', 'e', 'f')")), Seq("", "d", null)),

    TypeMapping(BinaryType, HanaDbType("BIT"),
      Seq(Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte), Array(0, 0, 0, 0, 0, 0, 0, 1).map(_.toByte), null)),
    TypeMapping(BinaryType, HanaDbType("BINARY", Some("BINARY(8)")),
      Seq(Array(0, 0, 0, 0, 0, 0, 0, 0).map(_.toByte), Array(0, 0, 0, 0, 0, 0, 0, 1).map(_.toByte), null)),
    TypeMapping(BinaryType, HanaDbType("VARBINARY", Some("VARBINARY(8)")), Seq(Array(0.toByte), Array(2.toByte), null)),
    TypeMapping(BinaryType, HanaDbType("TINYBLOB"), Seq(Array(0.toByte), Array(4.toByte), null)),
    TypeMapping(BinaryType, HanaDbType("MEDIUMBLOB"), Seq(Array(0.toByte), Array(8.toByte), null)),
    TypeMapping(BinaryType, HanaDbType("LONGBLOB"), Seq(Array(0.toByte), Array(16.toByte), null)),
    TypeMapping(BinaryType, HanaDbType("BLOB"), Seq(Array(0.toByte), Array(32.toByte), null)),

    TypeMapping(DateType, HanaDbType("DATE"), Seq(new Date(0), new Date(200), null)),
    TypeMapping(DateType, HanaDbType("YEAR"), Seq(Date.valueOf("1970-1-1"), Date.valueOf("1999-1-1"), null)),
    TypeMapping(StringType, HanaDbType("TIME"), Seq("12:00:00", "06:43:23", null)),
    //TODO: TIME types shouldn't be longs because HanaDb turns 64000L to 6:40:00

    // TIMESTAMP columns will turn NULL inputs into the current time, unless explicitly created as "TIMESTAMP NULL"
    TypeMapping(TimestampType, HanaDbType("TIMESTAMP", Some("TIMESTAMP NULL")),
      Seq(new Timestamp(0), new Timestamp(1449615940000L), null)),
    TypeMapping(TimestampType, HanaDbType("DATETIME"),
      Seq(new Timestamp(0), new Timestamp(1449615941000L), null))
  )

  "TestTypesSpec" should "do something with types" in {
    println("Creating all_types table")
    withStatement { stmt =>
      stmt.execute("DROP TABLE IF EXISTS all_types")
      stmt.execute(s"""
        CREATE TABLE all_types (
          id INT PRIMARY KEY AUTO_INCREMENT,
          ${types.map(_.HanaDbType.columnDefinition).mkString(", ")}
        )
      """)
    }

    println("Inserting values into table")
    val rows = types.head.values.indices.map { idx =>
      val id = idx.toLong + 1
      Row.fromSeq(Seq(id) ++ types.map(_.values(idx)))
    }
    val rdd = sc.parallelize(rows)

    val schema = ss
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map( "path" -> (dbName + ".all_types")))
      .load()
      .schema

    val HanaDbDF = ss.createDataFrame(rdd, schema)
    //HanaDbDF.saveToHanaDb("all_types")

    println("Testing round trip values")

    val all_types_df =  ss
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map( "path" -> (dbName + ".all_types")))
      .load()

    assert(TestUtils.equalDFs(HanaDbDF, all_types_df))

    println("Testing data returned from dataframe")
    val sparkDF = ss.createDataFrame(HanaDbDF.rdd, HanaDbDF.schema)
    val allDFs = (sparkDF, HanaDbDF)

    val aggregateFunctions: Seq[String => Column] = Seq(avg, count, max, min, sum)

    TestUtils.runQueries[DataFrame](allDFs, {
      case (df: DataFrame) => {
        types.flatMap { typeMapping =>
          val column = typeMapping.HanaDbType.columnName

          val basicQueries = Seq(
            df,
            df.select(column),
            df.select(df(column).as("test")),
            df.select(df(column), df(column)),
            df.orderBy(df(column).desc)
          )

          val filterQueries = typeMapping.sparkType match {
            //TODO currently DateType cannot be filtered
            case DateType => Nil
            case _ => {
              if(typeMapping.HanaDbType == HanaDbType("TIME")){
                // TODO: TIME types are currently not working properly
                // (see TODO above)
                // So the Time type would fail the null dataframe check
                // because the hardcoded data must be 0L at this point
                // in time
                Nil
              } else {
                val zeroValue = typeMapping.values.head
                Seq(
                  df.filter(df(column) > zeroValue),
                  df.filter(df(column) > zeroValue && df("id") > 0)
                )
              }
            }
          }

          val groupQueries = typeMapping.sparkType match {
            // SparkSQL does not support aggregating BinaryType
            case BinaryType => Nil
            // MySQL doesn't have the same semantics as SparkSQL for aggregating dates
            case DateType => {
              Seq(
                df.groupBy(df(column)).count()
              )
            }
            case _ => {
              Seq(
                df.groupBy(df(column)).count()
              ) ++ aggregateFunctions.map { fn =>
                df.groupBy(df("id")).agg(fn(column))
              }
            }
          }

          val complexQueries = typeMapping.sparkType match {
            // SparkSQL does not support aggregating BinaryType
            case BinaryType => Nil
            //TODO currently DateType cannot be filtered
            case DateType => Nil
            case _ => {
              if(typeMapping.HanaDbType == HanaDbType("TIME")){
                // TODO: TIME types are currently not working properly
                // (see TODO above)
                // So the Time type would fail the null dataframe check
                // because the hardcoded data must be 0L at this point
                // in time
                Nil
              } else {
                aggregateFunctions.map { fn =>
                  val zeroValue = typeMapping.values.head
                  df.select(df(column).as("foo"), df("id"))
                    .filter(col("foo") > zeroValue)
                    .groupBy("id")
                    .agg(fn("foo") as "bar")
                    .orderBy("bar")
                }
              }
            }
          }

          basicQueries ++ filterQueries ++ groupQueries ++ complexQueries
        }
      }
    })
  }
}
