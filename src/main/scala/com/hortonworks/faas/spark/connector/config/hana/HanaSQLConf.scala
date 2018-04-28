package com.hortonworks.faas.spark.connector.config.hana

import java.net.InetAddress

import com.hortonworks.faas.spark.connector.util.hana.HanaSQLConnectionInfo
import org.apache.spark.sql.RuntimeConfig


/**
 * Configuration for a HanaSQL cluster. By default these parameters are set by the corresponding
 * value in the Spark configuration.
 *
 * @param hanaHost Hostname of the HanaSQL hana Aggregator. Corresponds to "spark.HanaSQL.host"
 *                   in the Spark configuration.
 * @param hanaPort Port of the HanaSQL hana Aggregator. Corresponds to "spark.HanaSQL.port"
 *                   in the Spark configuration.
 * @param user Username to use when connecting to the HanaSQL hana Aggregator. Corresponds to
 *             "spark.HanaSQL.user" in the Spark configuration.
 * @param password Password to use when connecting to the HanaSQL hana Aggregator. Corresponds to
 *                 "sparkk.HanaSQL.password" in the Spark configuration.
 * @param defaultDBName The default database to use when connecting to the cluster. Corresponds to
 *                      "spark.HanaSQL.defaultDatabase" in the Spark configuration.
 */
case class HanaSQLConf(hanaHost: String,
                      hanaPort: Int,
                      user: String,
                      password: String,
                      defaultDBName: String
                      ) {

  val hanaConnectionInfo: HanaSQLConnectionInfo =
    HanaSQLConnectionInfo(hanaHost, hanaPort, user, password, defaultDBName)
}

object HanaSQLConf {
  val DEFAULT_PORT = 3306
  val DEFAULT_USER = "root"
  val DEFAULT_PASS = ""
  val DEFAULT_DATABASE = ""

  def getDefaultHost: String = InetAddress.getLocalHost.getHostAddress

  def apply(runtimeConf: RuntimeConfig): HanaSQLConf =
    HanaSQLConf(
      hanaHost = runtimeConf.get("spark.HanaSQL.host", getDefaultHost),
      hanaPort = runtimeConf.getOption("spark.HanaSQL.port").map(_.toInt).getOrElse(DEFAULT_PORT),
      user = runtimeConf.get("spark.HanaSQL.user", DEFAULT_USER),
      password = runtimeConf.get("spark.HanaSQL.password", DEFAULT_PASS),
      defaultDBName = runtimeConf.get("spark.HanaSQL.defaultDatabase",DEFAULT_DATABASE)
      )
}
