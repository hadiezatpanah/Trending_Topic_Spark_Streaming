package ir

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, pow, sqrt, trim, when}

package object brgroup {
  def nvl(ColIn: Column, ReplaceVal: Any): Column = {
    when(ColIn.isNull, lit(ReplaceVal)).otherwise(ColIn)
  }
  def nvlOrEmpty(ColIn: Column, ReplaceVal: Any): Column = {
    when(ColIn.isNull, lit(ReplaceVal))
      .when(trim(ColIn) === "", lit(ReplaceVal))
      .otherwise(ColIn)
  }
  def nvlOrEmptyCol(ColIn: Column, ColIn2: Column): Column = {
    when(ColIn.isNull, ColIn2)
      .when(trim(ColIn) === "", ColIn2)
      .otherwise(ColIn)
  }
  def std(avg: Column, sqrAvg: Column): Column = {
    sqrt(pow(avg - sqrAvg, lit(2)))
  }
}
