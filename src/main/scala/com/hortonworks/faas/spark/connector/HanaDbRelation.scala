package com.hortonworks.faas.spark.connector

import com.hortonworks.faas.spark.connector.hana.config.HanaDbCluster
import com.hortonworks.faas.spark.connector.hana.rdd.HanaDbRDD
import com.hortonworks.faas.spark.connector.hana.sql.{ColumnReference, TableIdentifier}
import com.hortonworks.faas.spark.connector.util.JDBCImplicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.mutable.ListBuffer

case class HanaDbQueryRelation(cluster: HanaDbCluster,
                                query: String,
                                namespaceName: Option[String],
                                sqlContext: SQLContext,
                                disablePartitionPushdown: Boolean,
                                enableStreaming: Boolean) extends BaseRelation with TableScan {

  override def schema: StructType = cluster.getQuerySchema(query)

  // Partition pushdown requires a namespace name.
  // If one was not supplied by the user in the option parameters,
  // Check the Spark configuration settings for "spark.HanaDb.defaultDatabase"
  val namespace: Option[String] = {
    namespaceName match {
      case Some(s) => namespaceName
      case None => {
          sqlContext.sparkSession.hanaDbConf.defaultDBName match {
          case "" => None
          case noneEmptyString => Some(noneEmptyString)
        }
      }
    }
  }

  def buildScan(): RDD[Row] = {
    HanaDbRDD(
      sqlContext.sparkContext,
      cluster,
      query,
      namespaceName = namespace,
      mapRow=_.toRow,
      disablePartitionPushdown=disablePartitionPushdown,
      enableStreaming=enableStreaming
    )
  }
}

case class HanaDbTableRelation(cluster: HanaDbCluster,
                                tableIdentifier: TableIdentifier,
                                sqlContext: SQLContext,
                                disablePartitionPushdown: Boolean,
                                enableStreaming: Boolean)
  extends BaseRelation
    with PrunedFilteredScan
     {

  val namespace: Option[String] = tableIdentifier.namespace

  override def schema: StructType = cluster.getQuerySchema(s"SELECT * FROM ${tableIdentifier.quotedString}")

  // TableScan
  def buildScan(): RDD[Row] = {
    val queryString = s"SELECT * FROM ${tableIdentifier.quotedString}"
    HanaDbRDD(sqlContext.sparkContext,
      cluster,
      queryString,
      namespaceName=namespace,
      mapRow=_.toRow,
      disablePartitionPushdown=disablePartitionPushdown,
      enableStreaming=enableStreaming)
  }

  //PrunedScan
  def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  //PrunedFilteredScan
  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val columnListAsString: String = {
      val sb = new StringBuilder()
      requiredColumns.foreach(x => sb.append(",").append(ColumnReference(x).quotedName))
      if (sb.isEmpty) "1" else sb.substring(1)
    }

    val params: ListBuffer[Any] = new ListBuffer[Any]()

    val whereClauseAsString: String = {
      filters
        .flatMap(compileFilter(_, params))
        .map(p => s"($p)").mkString(" AND ")
    }

    val finalWhereString: String = {
      if (whereClauseAsString.length > 0) {
        "WHERE " + whereClauseAsString
      } else {
        ""
      }
    }

    val queryString: String = s"SELECT ${columnListAsString} FROM ${tableIdentifier.quotedString} $finalWhereString"

    HanaDbRDD(sqlContext.sparkContext,
      cluster,
      queryString,
      sqlParams=params,
      namespaceName=namespace,
      mapRow=_.toRow,
      disablePartitionPushdown=disablePartitionPushdown,
      enableStreaming=enableStreaming)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    def isHandled(f: Filter): Boolean = {
      f match {
        case EqualTo(_, _)          |
          LessThan(_, _)            |
          GreaterThan(_, _)         |
          LessThanOrEqual(_, _)     |
          GreaterThanOrEqual(_, _)  |
          IsNotNull(_)              |
          IsNull(_)                 |
          StringStartsWith(_, _)    |
          StringEndsWith(_, _)      |
          StringContains(_, _)      |
          In(_, _)                  |
          Not(_)                    |
          Or(_, _)                  |
          And(_, _)                => true
        case _ => false
      }
    }
    filters.filter(!isHandled(_))
  }

  def compileFilter(f: Filter, params: ListBuffer[Any]): Option[String] = {
    Option(f match {
      case EqualTo(attr, value) => {
        params += value
        s"""`$attr` = ?"""
      }
      case LessThan(attr, value) => {
        params += value
        s"""`$attr` < ?"""
      }
      case GreaterThan(attr, value) => {
        params += value
        s"""`$attr` > ?"""
      }
      case LessThanOrEqual(attr, value) => {
        params += value
        s"""`$attr` <= ?"""
      }
      case GreaterThanOrEqual(attr, value) => {
        params += value
        s"""`$attr` >= ?"""
      }
      case IsNotNull(attr) => s"""`$attr` IS NOT NULL"""
      case IsNull(attr) => s"""`$attr` IS NULL"""
      case StringStartsWith(attr, value) => {
        params += s"${escapeWildcards(value)}%"
        s"""`$attr` LIKE ?"""
      }
      case StringEndsWith(attr, value) => {
        params += s"%${escapeWildcards(value)}"
        s"""`$attr` LIKE ?"""
      }
      case StringContains(attr, value) => {
        params += s"%${escapeWildcards(value)}%"
        s"""`$attr` LIKE ?"""
      }
      case In(attr, value) if value.isEmpty =>
        s"""CASE WHEN `$attr` IS NULL THEN NULL ELSE FALSE END"""
      case In(attr, value) => {
        params ++= value
        s"""`$attr` IN (${(1 to value.length).map(_ => "?").mkString(", ")})"""
      }
      case Not(f) => compileFilter(f, params).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        val or = Seq(f1, f2).flatMap(compileFilter(_, params))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, params))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  def escapeWildcards(s: String): String = {
    val percent = s.replaceAll("%", """\\%""")
    percent.replaceAll("_", """\\_""")
  }


}
