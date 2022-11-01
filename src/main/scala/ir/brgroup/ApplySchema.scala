package ir.brgroup

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, explode, from_json, substring, to_timestamp}
import org.apache.spark.sql.types.{LongType, StructType}
import org.ini4j.Profile


class ApplySchema (sparkSchemaSection: Profile.Section,streamData:DataFrame , sparkSession:SparkSession){
  def getDFWithSchema: DataFrame = {
    val jsData = Seq(
      (sparkSchemaSection.get("json.data",classOf[String]))
    )
    import sparkSession.implicits._
    val schema: StructType = sparkSession.read.json(jsData.toDS).schema

    val streamDataWithSchema =
        streamData.selectExpr("CAST(value AS STRING)",  "timestamp")
          .select(from_json(col("value"), schema).as("data"), col("timestamp"))

    val streamWithTimestamp: DataFrame =streamDataWithSchema
      .withColumn("EventDate",to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .select( explode(col("data.group.group_topics.topic_name")).as(sparkSchemaSection.get("grouped.column.name",classOf[String])),
        col("data.group.group_city").as("Group_City"),
        col("data.group.group_country").as("Group_Country"), col("EventDate"))

    streamWithTimestamp
  }
}



