package ir.brgroup

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import org.apache.spark.sql.functions.{col, count, sum, when, window}
import org.ini4j.Profile

class Aggregate (streamDF:DataFrame, aggSection:Profile.Section, schemaSection:Profile.Section) {

  def getAggregatedDF:DataFrame = {
    val aggDF:RelationalGroupedDataset = streamDF
//      .withWatermark("EventDate",
//        aggSection.get("watermark.duration",classOf[String]))
      .groupBy(
        window(col("EventDate"),
          aggSection.get("window.duration",classOf[String])
//          aggSection.get("window.sliding.duration",classOf[String])
        ),
        col(schemaSection.get("grouped.column.name",classOf[String]))
      )
    aggDF
      .agg(count(schemaSection.get("grouped.column.name",classOf[String]))
        .as(aggSection.get("aggregated.out.grouped.column.name",classOf[String])))
  }
}
