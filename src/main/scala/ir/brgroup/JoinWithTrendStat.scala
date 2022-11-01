package ir.brgroup

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col, trim}
import ir.brgroup.nvlOrEmptyCol
import org.apache.avro.generic.GenericData.StringType

class JoinWithTrendStat(streamDF:DataFrame, trendDf:DataFrame, streamJoinColumn:String, trendDfJoinColumn:String, joinType:String) {

  def getJoinedDF:DataFrame = {
    streamDF.join( trendDf, trim(streamDF(streamJoinColumn)).cast("string") === trim(trendDf(trendDfJoinColumn)).cast("string"), joinType )
      .select(streamDF.col("*")
        ,trendDf.col(trendDfJoinColumn).as( trendDfJoinColumn + "TDF")
        ,trendDf.col("avg")
        ,trendDf.col("sqr_Avg"))
  }
}


object JoinedOperation {
  def apply(streamDF: DataFrame, trendDf: DataFrame, streamJoinColumn: String, trendDfColumn: String, joinType: String): DataFrame = {
    val joinedDF = new JoinWithTrendStat( streamDF, trendDf, streamJoinColumn , trendDfColumn, joinType ).getJoinedDF
    joinedDF.select(nvlOrEmptyCol(col("Topic_Name"), col("Topic_NameTDF")).as("Topic_Name")
      , nvlOrEmpty(col("Topic_Count"), 0).as("Topic_Count")
      , nvlOrEmpty(col("avg"), 0).as("avg")
      , nvlOrEmpty(col("sqr_Avg"), 0).as("sqr_Avg"))
  }
}