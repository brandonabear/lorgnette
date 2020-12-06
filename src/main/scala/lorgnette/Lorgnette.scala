package lorgnette

import org.apache.spark.sql.types.{ArrayType, StructType, StructField}

object Lorgnette {

  def composeSchema(
    sc: StructType,
    selection: Array[String],
    delimiter: String = "_",
    prefix: Option[String] = None,
    cols: StructType = new StructType()
  ): StructType = {

    val result = sc.fields.foldLeft(cols)((columns, f) => {

      val flatName: String = prefix match {
        case Some(p) => s"$p$delimiter${f.name}"
        case _ => f.name
      }

//      val parentName: String = prefix match {
//        case Some(p) => p.split(delimiter).reverse.head
//        case _ => f.name
//      }

      f match {

          // StructType
        case StructField(_, structType: StructType, _, _) => {
          if (selection.exists(_.equals(flatName))) {
            columns.add(f.name, f.dataType)
          } else if (selection.exists(_.contains(f.name))) {
            columns.add(f.name, composeSchema(structType, selection, delimiter, Some(flatName)))
          } else {
            columns
          }
        }

          // ArrayType
        case StructField(_, ArrayType(structType: StructType, _), _, _) => {
          if (selection.exists(_.equals(flatName))) {
            columns.add(f.name, f.dataType)
          } else if (selection.exists(_.contains(f.name))) {
            columns.add(f.name, ArrayType(composeSchema(structType, selection, delimiter, Some(flatName))))
          } else {
            columns
          }
        }

          // Others
        case StructField(_, _, _, _) => if (selection.contains(flatName)) columns.add(f.name, f.dataType) else columns
      }
    })
    result
  }
}
