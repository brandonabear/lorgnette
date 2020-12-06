# lorgnette

[![Build Status](https://travis-ci.org/brandonabear/lorgnette.svg?branch=main)](https://travis-ci.org/brandonabear/lorgnette)
[![Coverage](https://codecov.io/gh/brandonabear/lorgnette/branch/main/graph/badge.svg?token=GUB05JXHS7)](https://codecov.io/gh/brandonabear/lorgnette)
[![Maintainability](https://api.codeclimate.com/v1/badges/b5b734410a4e57c7f453/maintainability)](https://codeclimate.com/github/brandonabear/lorgnette/maintainability)

`lorgnette` is a convenience utility for sub-setting complex Spark schemas with a list of xPaths.

```scala
import lorgnette.Lorgnette
import org.apache.spark.sql.types.{StructType, StructField, ArrayType, IntegerType, StringType}

// Given schema
val schema = StructType(
  StructField("id", IntegerType) :: 
  StructField("contacts", ArrayType(StructType(
    StructField("email", StringType) :: 
    StructField("phone", StringType) ::
    Nil
  ))) :: 
  Nil
)

// Provided subset
val selection = "id" :: "contacts_email" :: Nil

val newSchema = Lorgnette.composeSchema(schema, selection)
newSchema.printTreeString()
```