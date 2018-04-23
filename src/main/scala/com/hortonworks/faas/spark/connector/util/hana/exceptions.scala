package com.hortonworks.faas.spark.connector.util.hana

import com.hortonworks.faas.spark.connector.util.ConnectorException


class HANAConnectorException(msg: String) extends ConnectorException(msg)

class HANAConfigInvalidInputException(msg: String) extends HANAConnectorException(msg)

class HANAConfigMissingException(msg: String) extends HANAConnectorException(msg)

class HANAJdbcException(msg: String) extends HANAConnectorException(msg)

class HANAJdbcConnectionException(msg: String) extends HANAConnectorException(msg)

class HANAJdbcBadStateException(msg: String) extends HANAJdbcException(msg)