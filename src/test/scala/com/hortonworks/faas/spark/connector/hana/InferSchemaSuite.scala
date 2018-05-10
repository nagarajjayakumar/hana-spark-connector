package com.hortonworks.faas.spark.connector.hana

import java.text.SimpleDateFormat

import com.hortonworks.faas.spark.connector.util.InferSchema
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class InferSchemaSuite extends FunSuite {

  test("String fields types are inferred correctly from null types") {
    assert(InferSchema.inferField(NullType, "") == NullType)
    assert(InferSchema.inferField(NullType, null) == NullType)
    assert(InferSchema.inferField(NullType, "100000000000") == LongType)
    assert(InferSchema.inferField(NullType, "60") == IntegerType)
    assert(InferSchema.inferField(NullType, "3.5") == DoubleType)
    assert(InferSchema.inferField(NullType, "test") == StringType)
    assert(InferSchema.inferField(NullType, "2015-08-20 15:57:00") == TimestampType)
    assert(InferSchema.inferField(NullType, "True") == BooleanType)
    assert(InferSchema.inferField(NullType, "FAlSE") == BooleanType)
  }

  test("Null fields are handled properly when a nullValue is specified") {
    assert(InferSchema.inferField(NullType, "null", "null") == NullType)
    assert(InferSchema.inferField(StringType, "null", "null") == StringType)
    assert(InferSchema.inferField(LongType, "null", "null") == LongType)
    assert(InferSchema.inferField(IntegerType, "\\N", "\\N") == IntegerType)
    assert(InferSchema.inferField(DoubleType, "\\N", "\\N") == DoubleType)
    assert(InferSchema.inferField(TimestampType, "\\N", "\\N") == TimestampType)
    assert(InferSchema.inferField(BooleanType, "\\N", "\\N") == BooleanType)
  }

  test("String fields types are inferred correctly from other types") {
    assert(InferSchema.inferField(LongType, "1.0") == DoubleType)
    assert(InferSchema.inferField(LongType, "test") == StringType)
    assert(InferSchema.inferField(IntegerType, "1.0") == DoubleType)
    assert(InferSchema.inferField(DoubleType, null) == DoubleType)
    assert(InferSchema.inferField(DoubleType, "test") == StringType)
    assert(InferSchema.inferField(LongType, "2015-08-20 14:57:00") == TimestampType)
    assert(InferSchema.inferField(DoubleType, "2015-08-20 15:57:00") == TimestampType)
    assert(InferSchema.inferField(LongType, "True") == BooleanType)
    assert(InferSchema.inferField(IntegerType, "FALSE") == BooleanType)
    assert(InferSchema.inferField(TimestampType, "FALSE") == BooleanType)
  }

  test("Timestamp field types are inferred correctly via custom data format"){
    val formatter = new SimpleDateFormat("yyyy-mm")
    assert(
      InferSchema.inferField(TimestampType, "2015-08", dateFormatter = formatter) == TimestampType)
    formatter.applyPattern("yyyy")
    assert(
      InferSchema.inferField(TimestampType, "2015", dateFormatter = formatter) == TimestampType)
  }

  test("Timestamp field types are inferred correctly from other types") {
    assert(InferSchema.inferField(IntegerType, "2015-08-20 14") == StringType)
    assert(InferSchema.inferField(DoubleType, "2015-08-20 14:10") == StringType)
    assert(InferSchema.inferField(LongType, "2015-08 14:49:00") == StringType)
  }

  test("Merging Nulltypes should yeild Nulltype.") {
    assert(
      InferSchema.mergeRowTypes(Array(NullType),
        Array(NullType)).deep == Array(NullType).deep)
  }

  test("Boolean fields types are inferred correctly from other types") {
    assert(InferSchema.inferField(LongType, "Fale") == StringType)
    assert(InferSchema.inferField(DoubleType, "TRUEe") == StringType)
  }

  test("Type arrays are merged to highest common type") {
    assert(
      InferSchema.mergeRowTypes(Array(StringType),
        Array(DoubleType)).deep == Array(StringType).deep)
    assert(
      InferSchema.mergeRowTypes(Array(IntegerType),
        Array(LongType)).deep == Array(LongType).deep)
    assert(
      InferSchema.mergeRowTypes(Array(DoubleType),
        Array(LongType)).deep == Array(DoubleType).deep)
  }

}