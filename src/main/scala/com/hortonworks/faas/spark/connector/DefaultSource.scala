package com.hortonworks.faas.spark.connector

import com.hortonworks.faas.spark.connector.hana.config.HanaDbCluster
import com.hortonworks.faas.spark.connector.hana.sql.TableIdentifier
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}


/**
  * This class is a proxy for the actual implementation in org.apache.spark.
  * It allows you to write data to HanaDb via the Spark RelationProvider API.
  *
  * Example:
  *   df.write.format("com.hortonworks.faas.spark.connector").save("foo.bar")
  */
class DefaultSource extends RelationProvider
   with DataSourceRegister{

  override def shortName(): String = "hanadb"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val conf = sqlContext.sparkSession.hanaDbConf

    val cluster = HanaDbCluster(conf)

    val enableStreaming: Boolean = DefaultSource.getValueAsBoolean(parameters, "enableStreaming")

    val disablePartitionPushdown : Boolean = DefaultSource.getValueAsBoolean(parameters, "disablePartitionPushdown")

    parameters.get("path") match {
      case Some(path) => HanaDbTableRelation(cluster, DefaultSource.getTableIdentifier(path), sqlContext, disablePartitionPushdown, enableStreaming)

      case None => parameters.get("query") match {
        case Some(query) => HanaDbQueryRelation(cluster, query, parameters.get("database"), sqlContext, disablePartitionPushdown, enableStreaming)
        case None => throw new UnsupportedOperationException("Must specify a path or query when loading a HanaDb DataSource")
      }
    }
  }


}

object DefaultSource {
  def getTableIdentifier(path: String): TableIdentifier = {
    val sparkTableIdent = CatalystSqlParser.parseTableIdentifier(path)
    TableIdentifier(sparkTableIdent.table, sparkTableIdent.database)
  }

  def getValueAsBoolean(parameters: Map[String, String], key: String): Boolean = {
    parameters.get(key) match {
      case None => false
      case Some(s) => {
        s.toLowerCase.trim match {
          case "true" => true
          case _ => false
        }
      }
    }
  }
}
