package com.hortonworks.faas.spark.connector.hana.config

import java.net.InetAddress

import com.hortonworks.faas.spark.connector.hana.util.HanaDbConnectionInfo
import org.apache.spark.sql.RuntimeConfig


/**
 * Configuration for a HanaDb cluster. By default these parameters are set by the corresponding
 * value in the Spark configuration.
 *
 * @param hanaHost Hostname of the HanaDb dataframe Aggregator. Corresponds to "spark.hanadb.host"
 *                   in the Spark configuration.
 * @param hanaPort Port of the HanaDb dataframe Aggregator. Corresponds to "spark.hanadb.port"
 *                   in the Spark configuration.
 * @param user Username to use when connecting to the HanaDb dataframe Aggregator. Corresponds to
 *             "spark.hanadb.user" in the Spark configuration.
 * @param password Password to use when connecting to the HanaDb dataframe Aggregator. Corresponds to
 *                 "sparkk.hanadb.password" in the Spark configuration.
 * @param defaultDBName The default database to use when connecting to the cluster. Corresponds to
 *                      "spark.hanadb.defaultDatabase" in the Spark configuration.
 */
case class HanaDbConf(hanaHost: String,
                      hanaPort: Int,
                      user: String,
                      password: String,
                      defaultDBName: String
                      ) {

  val hanaConnectionInfo: HanaDbConnectionInfo =
    HanaDbConnectionInfo(hanaHost, hanaPort, user, password, defaultDBName)
}

object HanaDbConf {
  val DEFAULT_PORT = 3306
  val DEFAULT_USER = "root"
  val DEFAULT_PASS = ""
  val DEFAULT_DATABASE = ""
  val DEFAULT_PUSHDOWN_ENABLED = true
  val DEFAULT_DISABLE_PARTITION_PUSHDOWN = false

  def getDefaultHost: String = InetAddress.getLocalHost.getHostAddress

  def apply(runtimeConf: RuntimeConfig): HanaDbConf =
    HanaDbConf(
      hanaHost = runtimeConf.get("spark.hanadb.host", getDefaultHost),
      hanaPort = runtimeConf.getOption("spark.hanadb.port").map(_.toInt).getOrElse(DEFAULT_PORT),
      user = runtimeConf.get("spark.hanadb.user", DEFAULT_USER),
      password = runtimeConf.get("spark.hanadb.password", DEFAULT_PASS),
      defaultDBName = runtimeConf.get("spark.hanadb.defaultDatabase",DEFAULT_DATABASE)
      )
}
