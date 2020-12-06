# lorgnette

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