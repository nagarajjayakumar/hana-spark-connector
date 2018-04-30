package com.hortonworks.faas.spark.connector.hana.sql

case class TableIdentifier(table: String, namespace: Option[String] = None) {
  def withnamespace(namespace: String): TableIdentifier = this.copy(namespace = Some(namespace))

  override def toString: String = quotedString

  def quotedString: String =
    namespace match {
      case Some(value) => s""""$value"."$table""""
      case None => table
    }

  def toSeq: Seq[String] = namespace.toSeq :+ table

  /**
    * Returns a fully qualified table name.
    *
    * @param namespace The table namespace
    * @param tableName The table name
    * @return the fully qualified table name
    */
  def tableWithNamespace(namespace: Option[String], tableName: String) =
    namespace match {
      case Some(value) => s""""$value"."$tableName""""
      case None => tableName
    }
}

object TableIdentifier {
  def apply(namespace: String, tableName: String): TableIdentifier =
    TableIdentifier(tableName, Some(namespace))
}
