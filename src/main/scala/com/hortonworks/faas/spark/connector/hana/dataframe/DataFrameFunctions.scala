package com.hortonworks.faas.spark.connector.hana.dataframe

import com.hortonworks.faas.spark.connector.hana.config
import com.hortonworks.faas.spark.connector.hana.config.{HanaDbCluster, HanaDbConf}
import org.apache.spark.sql.DataFrame

class DataFrameFunctions(df: DataFrame) {

  def getHanaDbConf: HanaDbConf = HanaDbConf(df.sparkSession.conf)

  def getHanaDbCluster: HanaDbCluster = config.HanaDbCluster(getHanaDbConf)

  

}
