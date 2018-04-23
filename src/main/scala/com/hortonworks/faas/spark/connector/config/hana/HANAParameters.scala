package com.hortonworks.faas.spark.connector.config.hana

import java.util

import com.hortonworks.faas.spark.connector.config.BaseParameters
import com.hortonworks.faas.spark.connector.util.hana.HANAConfigMissingException

import scala.collection.JavaConversions._

object HANAParameters extends BaseParameters {

  override def getConfig(props: util.Map[String, String]): HANAConfig = {
    super.getConfig(props)

    if (props.get("connection.url") == null) {
      throw new HANAConfigMissingException("Mandatory parameter missing: " +
        " HANA DB Jdbc url must be specified in 'connection.url' parameter")
    }

    if (props.get("connection.user") == null) {
      throw new HANAConfigMissingException("Mandatory parameter missing: " +
        " HANA DB user must be specified in 'connection.user' parameter")
    }

    if (props.get("connection.password") == null) {
      throw new HANAConfigMissingException("Mandatory parameter missing: " +
        " HANA DB password must be specified in 'connection.password' parameter")
    }

    HANAConfig(props.toMap)
  }
}