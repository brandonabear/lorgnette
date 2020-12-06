package lorgnette

import org.scalatest._
import org.apache.spark.sql.types._
import lorgnette.Lorgnette


trait SchemaFixture {
  val fullSchema = StructType(
    StructField("id", StringType) ::
    StructField("name", StringType) ::
    StructField("valid", BooleanType) ::
    StructField("person", StructType(
      StructField("firstName", StringType) ::
      StructField("lastName", StringType) ::
      StructField("age", StringType) ::
      StructField("typeCd", StructType(
        StructField("srcCd", StringType) ::
        StructField("value", StringType) ::
        Nil
      )) ::
      Nil
    )) ::
    StructField("children", ArrayType(StructType(
      StructField("firstName", StringType) ::
      StructField("lastName", StringType) ::
      StructField("typeCd", StructType(
        StructField("srcCd", StringType) ::
        StructField("value", StringType) ::
        Nil
      )) ::
      Nil
    ))) ::
    StructField("features", MapType(StringType, StringType)) ::
    Nil
  )
}


class LorgnetteSuite extends FlatSpec with SchemaFixture {

  "Selecting non-complex fields" should "drop structs/arrays/maps" in {
    val selection = Array("id", "name", "valid")
    val expectedSchema = StructType(
      StructField("id", StringType) ::
      StructField("name", StringType) ::
      StructField("valid", BooleanType) ::
      Nil
    )
    assert(Lorgnette.composeSchema(fullSchema, selection) == expectedSchema)
  }

  "Selecting nested fields in structs" should "maintain struct parent lineage" in {
    val selection = Array("id", "person_typeCd_value")
    val expectedSchema = StructType(
      StructField("id", StringType) ::
      StructField("person", StructType(
        StructField("typeCd", StructType(
          StructField("value", StringType) ::
          Nil
        )) ::
        Nil
      )) ::
      Nil
    )
    assert(Lorgnette.composeSchema(fullSchema, selection) == expectedSchema)
  }

  "Selecting nested fields in arrays" should "maintain array parent lineage" in {
    val selection = Array("id", "children_typeCd_value")
    val expectedSchema = StructType(
      StructField("id", StringType) ::
      StructField("children", ArrayType(StructType(
        StructField("typeCd", StructType(
          StructField("value", StringType) ::
          Nil
        )) ::
        Nil
      ))) ::
      Nil
    )
    assert(Lorgnette.composeSchema(fullSchema, selection) == expectedSchema)
  }

  "Selecting struct parent" should "return all nested fields" in {
    val selection = Array("id", "children_typeCd")
    val expectedSchema = StructType(
      StructField("id", StringType) ::
      StructField("children", ArrayType(StructType(
        StructField("typeCd", StructType(
          StructField("srcCd", StringType) ::
          StructField("value", StringType) ::
          Nil
        )) ::
        Nil
      ))) ::
      Nil
    )
    assert(Lorgnette.composeSchema(fullSchema, selection) == expectedSchema)
  }

  "Selecting array parent" should "return all nested fields" in {
    val selection = Array("id", "children")
    val expectedSchema = StructType(
      StructField("id", StringType) ::
      StructField("children", ArrayType(StructType(
        StructField("firstName", StringType) ::
        StructField("lastName", StringType) ::
        StructField("typeCd", StructType(
          StructField("srcCd", StringType) ::
          StructField("value", StringType) ::
          Nil
        )) ::
        Nil
      ))) ::
      Nil
    )
    assert(Lorgnette.composeSchema(fullSchema, selection) == expectedSchema)
  }
}
