package com.hortonworks.faas.spark.connector.util

class ConnectorException(msg: String) extends Exception(msg)

class SchemaNotMatchedException(msg: String) extends ConnectorException(msg)