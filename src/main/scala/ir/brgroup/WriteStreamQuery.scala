package ir.brgroup

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, concat, current_timestamp, date_format, expr, greatest, lit, md5, pow, rand, row_number, trim, when}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.ini4j.Profile

class WriteStreamQuery(streamDF: DataFrame, writeSection: Profile.Section, sparkSession: SparkSession, queryName: String) {

  def getWriteStream: StreamingQuery = {

    streamDF
      .withColumn("start_date", date_format(col("window.start"), "yyyyMMddHHmmss"))
      .withColumn("end_date", date_format(col("window.end"), "yyyyMMddHHmmss"))
      .withColumn("hour", date_format(col("window.start"), "HH"))
      .drop(col("window"))
      .writeStream
      .option("checkpointLocation", writeSection.get("option.checkpointLocation", classOf[String]) + "/" + queryName)
      .trigger(Trigger.ProcessingTime(writeSection.get("trigger.processingTime", classOf[String])))
      .outputMode(writeSection.get("output.mode", classOf[String]))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

      if (!batchDF.head(1).isEmpty) {
//        batchDF.show(false)
//        println(batchId)
//        println(queryName + ": " + batchDF.count())
        import sparkSession.implicits._
        batchDF.persist()

        if(!sparkSession.catalog.tableExists("trend_table") ) {
          if (batchDF.groupBy($"Topic_Name").count.filter($"count" > 1).count() == 0) {
            val processedDF: DataFrame = batchDF.withColumn("avg", col("Topic_Count").cast(DecimalType(38,3)))
              .withColumn("sqr_Avg", pow(col("Topic_Count"), lit(2)).cast(DecimalType(38,3)))
            processedDF.select(col("Topic_Name"), col("avg"), col("sqr_Avg"))
              .write.mode("overwrite").saveAsTable("trend_table")
          }
        } else {
          sparkSession.sparkContext.setCheckpointDir(writeSection.get("option.checkpointLocation", classOf[String]) + "/SC")
          sparkSession.sql("refresh TABLE trend_table")
          val trendDf: DataFrame = sparkSession.read.table("trend_table").checkpoint()
          val joinedDF = JoinedOperation.apply(batchDF, trendDf, "Topic_Name", "Topic_Name", "fullouter")
          val processedDF: DataFrame = joinedDF.select(
            when(col("avg") === 0 && col("sqr_Avg") === 0, col("Topic_Count"))
            .otherwise(col("avg") * lit(writeSection.get("trend.decay", classOf[Float])) +
              col("Topic_Count") * lit(1 - writeSection.get("trend.decay", classOf[Float])))
          .as("avg")
          , when(col("avg") === 0 && col("sqr_Avg") === 0, pow(col("Topic_Count"), lit(2)).cast(DecimalType(38,3)))
              .otherwise(col("sqr_Avg") * lit(writeSection.get("trend.decay", classOf[Float])) +
              pow(col("Topic_Count"), lit(2)).cast(DecimalType(38,3)) * lit(1 - writeSection.get("trend.decay", classOf[Float])))
                .as("sqr_Avg")
            , col("Topic_Name")
            , col("Topic_Count")
          )
          processedDF.select(col("Topic_Name"), col("avg"), col("sqr_Avg"))
            .write.mode("overwrite").saveAsTable("trend_table")
          val finalDF: DataFrame = processedDF.select(col("Topic_Name")
            , when(std(col("avg"), col("sqr_Avg")).cast(DecimalType(38,3)) === lit(0).cast(DecimalType(38,3)) , col("Topic_Count") - col("avg") )
              .otherwise((col("Topic_Count") - col("avg")) / std(col("avg"), col("sqr_Avg")) )
              .as("Trend_Score")
          )
          val windowSpec = Window.orderBy(col("Trend_Score").desc)
          val trendDF: DataFrame =finalDF.withColumn("Trend_Rank", row_number().over(windowSpec).cast(IntegerType)).orderBy(col("Trend_Rank").asc)
            .withColumn("Load_Date",current_timestamp().as("Load_Date"))
            .filter(col("Trend_Rank")<= writeSection.get("trend.output.num", classOf[Int]))
          trendDF
            .select(writeSection.get("target.table.columns", classOf[String]).split(",").map(col): _*)
            .write
            .format("jdbc")
//            .option("truncate", "true")
            .option("url",writeSection.get("target.db.url", classOf[String]))
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", writeSection.get("target.table", classOf[String]))
            .option("user", writeSection.get("target.db.user", classOf[String]))
            .option("password", writeSection.get("target.db.pass", classOf[String]))
            .option("numPartitions",writeSection.get("target.conn.num.partition", classOf[Int]))
            .mode(writeSection.get("target.table.mode", classOf[String]))
            .save()
        }
        batchDF.unpersist()
      }
    }
    .queryName(queryName)
    .start()
  }
}




