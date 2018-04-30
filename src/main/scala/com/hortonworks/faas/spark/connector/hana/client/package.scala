package com.hortonworks.faas.spark.connector.hana

package object client {

  // Variables from the spark context have to be prefixed by
  protected[client] val CONF_PREFIX = "spark.dataframe."

  protected[client] val PARAMETER_PATH = "tablepath"
  protected[client] val PARAMETER_DBSCHEMA = "dbschema"
  protected[client] val PARAMETER_HOST = "host"
  protected[client] val PARAMETER_INSTANCE_ID = "instance"
  protected[client] val PARAMETER_USER = "user"
  protected[client] val PARAMETER_PASSWORD = "passwd"
  protected[client] val PARAMETER_PARTITION_COLUMN = "partitioningColumn"
  protected[client] val PARAMETER_NUM_PARTITIONS = "numberOfPartitions"
  protected[client] val PARAMETER_MAX_PARTITION_SIZE = "maximumPartitionSize"
  protected[client] val PARAMETER_BATCHSIZE = "batchsize"
  protected[client] val DEFAULT_BATCHSIZE = 1000
  protected[client] val DEFAULT_NUM_PARTITIONS = 1
  protected[client] val PARAMETER_SCHEMA = "schema"
  protected[client] val PARAMETER_LOWER_BOUND = "lowerBound"
  protected[client] val PARAMETER_UPPER_BOUND = "upperBound"
  protected[client] val MAX_PARTITION_SIZE = 100000
  protected[client] val PARAMETER_COLUMN_TABLE = "columnStore"
  protected[client] val DEFAULT_COLUMN_TABLE = false
  protected[client] val PARAMETER_TABLE_PATTERN = "tablePattern"
  protected[client] val DEFAULT_TABLE_PATTERN = "%"
  protected[client] val PARAMETER_VIRTUAL_TABLE = "virtual"
  protected[client] val DEFAULT_VIRTUAL_TABLE = true
  protected[client] val PARAMETER_TENANT_DATABASE = "tenantdatabase"
  protected[client] val PARAMETER_PORT = "port"
  protected[client] val DEFAULT_SINGLE_CONT_HANA_PORT = "15"
  protected[client] val DEFAULT_MULTI_CONT_HANA_PORT = "13"

  protected[client] val CONSTANT_STORAGE_TYPE  = "STORAGE_TYPE"
  protected[client] val CONSTANT_COLUMN_STORE  = "COLUMN STORE"
  protected[client] val CONSTANT_ROW_STORE  = "ROW STORE"


}
