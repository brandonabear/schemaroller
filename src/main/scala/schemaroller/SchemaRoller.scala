package schemaroller

import org.apache.spark.sql.functions.{col, explode_outer, max, size}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object SchemaRoller {

  /**
   * Star-expands a struct column and prepends with parent column name.
   */
  def expandStruct(
    structName: String,
    delimiter: String = "_")
    (df: DataFrame): DataFrame = {
    val innerColumns = df.select(s"$structName.*").columns
    df
      .select(col("*") +:
        innerColumns.map(c => col(s"$structName.${c}").as(s"${structName}${delimiter}${c}")): _*)
      .drop(structName)
  }

  /**
   * Explodes an array column to rows and prepends with parent column name.
   */
  def explodeArray(arrayName: String, delimiter: String = "_")(df: DataFrame): DataFrame = {
    df
      .withColumn(arrayName, explode_outer(col(arrayName)))
      .transform(this.expandStruct(arrayName, delimiter))
      .drop(arrayName)
  }

  /**
   * Pivots elements of an array to columns and prepends with parent column name and array index.
   */
  def expandArray(arrayName: String, delimiter: String = "_", maxElements: Option[Int] = None)(df: DataFrame): DataFrame = {

    // Do not calculate if maxElements is provided
    val arraySize = if (maxElements.isEmpty) {
      df.agg(max(size(col(arrayName)))).collect()(0)(0).toString.toInt
    } else {
      1
    }
    val arrayCap = maxElements.getOrElse(arraySize)

    df
      .select(col("*") +:
        (0 until arrayCap).map(i => col(arrayName).getItem(i).as(s"${arrayName}${delimiter}${i + 1}")): _*)
      .drop(arrayName)
      .transform(this.recursiveFlatten())
  }

  /**
   * Recursively expand/explode all columns.
   *
   * Default behavior is to only descend into structs.
   *
   * Example usage:
   * df.transform(Extractor.recursiveFlatten())
   */
  def recursiveFlatten(
    prefixDelimiter: String = "_",
    structOnly: Boolean = true)
    (df: DataFrame): DataFrame = {


    /**
     * Flatten and/or explode a single level.
     */
    def flattenAndExplodeOne(
      sc: StructType,
      parent: Option[Column] = None,
      prefix: Option[String] = None,
      cols: Array[(DataType, Column)] = Array[(DataType, Column)]()): Array[(DataType, Column)] = {

      val result = sc.fields.foldLeft(cols)((columns, f) => {

        val tempCol: Column = parent match {
          case Some(p) => p.getItem(f.name)
          case _ => col(f.name)
        }

        val flatName: String = prefix match {
          case Some(pref) => s"${pref}${prefixDelimiter}${f.name}"
          case _ => f.name
        }

        f.dataType match {
          case st: StructType => flattenAndExplodeOne(st, Some(tempCol), Some(flatName), columns)
          case at: ArrayType => {
            if (columns.exists(_._1.isInstanceOf[ArrayType])) {
              columns :+ ((at, tempCol.as(flatName)))
            } else {
              if (structOnly) {
                columns :+ ((at, tempCol.as(flatName)))
              } else {
                columns :+ ((at, explode_outer(tempCol).as(flatName)))
              }
            }
          }
          case dt => columns :+ ((dt, tempCol.as(flatName)))
        }
      })
      result
    }

    var flatDf = df
    while (isComplex(flatDf.schema, structOnly)) {
      val newColumns = flattenAndExplodeOne(flatDf.schema).map(_._2)
      flatDf = flatDf.select(newColumns: _*)
    }

    flatDf
  }


  private def isStruct(sf: StructField): Boolean = {
    sf.dataType.isInstanceOf[StructType]
  }

  private def isArray(sf: StructField): Boolean = {
    sf.dataType.isInstanceOf[ArrayType]
  }

  private def isComplex(sc: StructType, structOnly: Boolean): Boolean = {
    if (structOnly) {
      sc.fields.exists(f => isStruct(f))
    } else {
      sc.fields.exists(f => isStruct(f) | isArray(f))
    }
  }
}
