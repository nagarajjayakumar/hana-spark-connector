package com.hortonworks.faas.spark


import com.hortonworks.faas.spark.connector.hana.config.{HanaDbCluster, HanaDbConf}
import com.hortonworks.faas.spark.connector.hana.dataframe.{DataFrameFunctions, TypeConversions}
import com.hortonworks.faas.spark.connector.hana.sql.ColumnDefinition
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

package object connector {

  /* HanaDb functions on DataFrames */
  implicit def dataFrameFunctions(df: DataFrame): DataFrameFunctions = new DataFrameFunctions(df)

  implicit def sparkSessionFunctions(sparkSession: SparkSession): SparkSessionFunctions = new SparkSessionFunctions(sparkSession)

  class SparkSessionFunctions(sparkSession: SparkSession) extends Serializable {

    def hanaDbConf: HanaDbConf = HanaDbConf(sparkSession.conf)

    def setDatabase(dbName: String): Unit = {
      sparkSession.conf.set("spark.HanaDb.defaultDatabase", dbName)
    }
    def getDatabase: String = hanaDbConf.defaultDBName

    def getHanaDbCluster: HanaDbCluster = HanaDbCluster(hanaDbConf)
  }

  implicit def schemaFunctions(schema: StructType): SchemaFunctions = new SchemaFunctions(schema)

  class SchemaFunctions(schema: StructType) extends Serializable {
    def toHanaDbColumns: Seq[ColumnDefinition] = {
      schema.map(s => {
        ColumnDefinition(
          s.name,
          TypeConversions.DataFrameTypeToHanaDbTypeString(s.dataType),
          s.nullable
        )
      })
    }
  }
}
