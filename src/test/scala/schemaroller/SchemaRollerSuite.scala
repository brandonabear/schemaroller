package schemaroller

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._


trait LocalSparkContext {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("Schemaroller Unit Tests")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}

trait SampleStructFields {
  val testStructField: StructField = StructField("is_a_struct", StructType(Seq(
    StructField("nested_field", StringType, true)
  )), true)

  val testArrayField: StructField = StructField("is_an_array", ArrayType(StructType(Seq(
    StructField("nested_field", StringType, true)
  ))), true)

  val testStringField: StructField = StructField("is_a_string", StringType, true)
}

trait DataFixture extends LocalSparkContext {
  case class StatusCd(srcCd: String, value: String)
  case class Customer(customerId: String, statusCd: StatusCd)

  case class Wheel(id: String)
  case class Vehicle(vin: String, wheels: Array[Wheel])

  case class Root(customer: Customer, vehicles: Array[Vehicle])

  val df: DataFrame = spark.createDataFrame(Seq(
   Root(
     Customer("C1", StatusCd("1", "01")),
     Array(
       Vehicle("V1", Array(
         Wheel("FL"), Wheel("FR"), Wheel("BL"), Wheel("BR")
       )),
       Vehicle("V2", Array(
         Wheel("FL"), Wheel("FR"), Wheel("BL"), Wheel("BR")
       ))
     )
   ),
    Root(
    Customer("C2", StatusCd("2", "02")),
    Array(
      Vehicle("V1", Array(
        Wheel("FC"), Wheel("BL"), Wheel("BR")
      ))
    ))
  ))
}

class SchemaRollerSuite
  extends FlatSpec
  with LocalSparkContext
  with DataFixture
  with SampleStructFields
  with PrivateMethodTester {

  "These tests" should "share a valid SparkSession instance" in {
    assert(spark.isInstanceOf[SparkSession])
  }

  "expandStruct" should "expand one level of provided struct column" in {
    // We expect that nested fields in StatusCd will remain
    // Transformed fields appear at the end of the schema
    val expectedSchema = StructType(
      StructField("vehicles", ArrayType(StructType(
        StructField("vin", StringType) ::
          StructField("wheels", ArrayType(StructType(
            StructField("id", StringType) ::
              Nil
          ))) ::
          Nil
      ))) ::
        StructField("customer_customerId", StringType) ::
        StructField("customer_statusCd", StructType(
          StructField("srcCd", StringType) ::
            StructField("value", StringType) ::
            Nil
        )) ::
        Nil
    )

    val result = df.transform(SchemaRoller.expandStruct("customer"))
    assert(result.schema == expectedSchema)
    assert(result.count == df.count)
  }

  "explodeArray" should "explode a single array to rows and expand all structs" in {

    // We expect the vehicle array to be flattened.
    val expectedSchema = StructType(
      StructField("customer", StructType(
        StructField("customerId", StringType) ::
          StructField("statusCd", StructType(
            StructField("srcCd", StringType) ::
              StructField("value", StringType) ::
              Nil
          )) ::
          Nil
      )) ::

        StructField("vehicles_vin", StringType) ::
        StructField("vehicles_wheels", ArrayType(StructType(
          StructField("id", StringType) ::
            Nil
        ))) ::
        Nil
    )

    val result = df.transform(SchemaRoller.explodeArray("vehicles"))
    assert(result.schema == expectedSchema)
    assert(result.count >= df.count)
  }

  "isStruct" should "identify if StructField datatype is StructType" in {
      val isStruct = PrivateMethod[Boolean]('isStruct)
      val tests = Seq(
        (testStructField, true),
        (testArrayField, false),
        (testStringField, false)
      )
      for ((field, expectation) <- tests) {
        val result = SchemaRoller invokePrivate isStruct(field)
        assert(result == expectation)
      }
    }

  "isArray" should "Identify if StructField datatype is ArrayType" in {
    val isArray = PrivateMethod[Boolean]('isArray)
    val tests = Seq(
      (testStructField, false),
      (testArrayField, true),
      (testStringField, false)
    )
    for ((field, expectation) <- tests) {
      val result = SchemaRoller invokePrivate isArray(field)
      assert(result == expectation)
    }
  }

  "isComplex" should "Identify if StructField datatype is StructType when structOnly" in {
    val isComplex = PrivateMethod[Boolean]('isComplex)

    val resultStructOnly = SchemaRoller invokePrivate isComplex(df.schema, true)
    assert(resultStructOnly)

    val resultStructArray = SchemaRoller invokePrivate isComplex(df.schema, false)
    assert(resultStructArray)
  }

}