package com.hortonworks.faas.spark.connector.config.hana

import java.sql.Connection

import com.hortonworks.faas.spark.connector.client.hana.{HANAJdbcClient, metaAttr}
import com.hortonworks.faas.spark.connector.dataframe.hana.TypeConversions
import com.hortonworks.faas.spark.connector.util.{ExecuteWithExceptions, WithCloseables}
import com.hortonworks.faas.spark.connector.util.hana.{HANAJdbcException, HanaSQLConnectionInfo}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}


case class HanaSQLCluster(conf: HanaSQLConf)  {
  val ER_DUP_FIELDNAME = 1060

  def getHanaInfo: HanaSQLConnectionInfo = conf.hanaConnectionInfo

  def jdbcClient : HANAJdbcClient = HANAJdbcClient(conf)

  def withHanaConn[T]: ((Connection) => T) => T =
    HanaSQLConnectionPool.withConnection(getHanaInfo)

  def getConnection: Connection = {
    HanaSQLConnectionPool.connect(getHanaInfo)
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

