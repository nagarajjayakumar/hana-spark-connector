package com.hortonworks.faas.spark.connector.hana.config

import java.sql.Connection

import com.hortonworks.faas.spark.connector.hana.client.HanaDbJdbcClient
import com.hortonworks.faas.spark.connector.hana.dataframe.TypeConversions
import com.hortonworks.faas.spark.connector.hana.util.{HANAJdbcException, HanaDbConnectionInfo}
import com.hortonworks.faas.spark.connector.util.{ExecuteWithExceptions, WithCloseables}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

case class HanaDbCluster(conf: HanaDbConf)  {
  val ER_DUP_FIELDNAME = 1060

  def getHanaInfo: HanaDbConnectionInfo = conf.hanaConnectionInfo

  def jdbcClient : HanaDbJdbcClient = HanaDbJdbcClient(conf)

  def withHanaConn[T]: ((Connection) => T) => T =
    HanaDbConnectionPool.withConnection(getHanaInfo)

  def getConnection: Connection = {
    HanaDbConnectionPool.connect(getHanaInfo)
  }


  def getQuerySchema(query: String, queryParams: Seq[Any]=Nil): StructType = {
    val metaDataBuilder : MetadataBuilder = new MetadataBuilder
    WithCloseables(getConnection) { conn =>
      WithCloseables(conn.createStatement()) { stmt =>
        WithCloseables(stmt.executeQuery(s"SELECT * FROM ($query)  lzalias limit 0")) { rs =>
          val metadata = rs.getMetaData
          val columnCount = metadata.getColumnCount

          StructType(
            Range(0, columnCount)
              .map(i => StructField(
                metadata.getColumnName(i + 1),
                TypeConversions.JDBCTypeToDataFrameType(metadata, i + 1),
                true, metaDataBuilder.build())
              )
          )
        }
      }
    }
  }

  def getQuerySchema(tableName: String, namespace: Option[String]): StructType = {
    ExecuteWithExceptions[StructType, Exception, HANAJdbcException](
      new HANAJdbcException(s"Fetching of metadata for $tableName failed")) { () =>
      val fullTableName = jdbcClient.tableWithNamespace(namespace, tableName)
      val metaDataBuilder : MetadataBuilder = new MetadataBuilder

      WithCloseables(getConnection) { conn =>
        WithCloseables(conn.createStatement()) { stmt =>
          WithCloseables(stmt.executeQuery(s"SELECT * FROM $fullTableName LIMIT 0")) { rs =>
            val metadata = rs.getMetaData
            val columnCount = metadata.getColumnCount

            StructType(
              Range(0, columnCount)
                .map(i => StructField(
                  metadata.getColumnName(i + 1),
                  TypeConversions.JDBCTypeToDataFrameType(metadata, i + 1),
                  true, metaDataBuilder.build())
                )
            )
          }
        }
      }
    }
  }


}

