package ir.brgroup

import MEETUPS_TREND.sparkMonitoringSection
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, FileNotFoundException, IOException}
import org.ini4j._
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.functions.lit

//spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 target/scala-2.11/CEP_CDR_USAGE-assembly-1.8.jar src/main/resources/log4j.properties  src/main/scala/ir/brtech/config/config_local.ini

object MEETUPS_TREND extends App with Logger {

  //  get input parameters
  val log4Path : String =  args(0)
  val iniFilePath: String = args(1)

  //  Setup log4j class
  PropertyConfigurator.configure(log4Path)
  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.INFO)

  info("starting CDR Usage!")
  //Try to read config file
  val ini :Wini =
    try {
      new Wini(new File(iniFilePath))
    }
    catch {
      case x: FileNotFoundException =>
        println("Exception: File missing")
        error("Exception - ",x)
        null
      case x: IOException   =>
        println("Input/output Exception")
        error("Exception - ",x)
        null
    }

  //Read all section from config file : Common Sections
  val sparkSessionSection = ini.get("SparkSession")

  //  Read input stream sections
  val sparkInputStreamMeetupRSVPSection = ini.get("MeetupRSVPReadStream")

  //  read all schema sections
  val sparkSchemaMeetupRSVPSection = ini.get("JsonSchema")

  //  Read Aggregation section
  val sparkAggregationSection = ini.get("Aggregation")

  val sparkWriteStreamSection = ini.get("WriteStream")

  info("Ini file has been loaded Successfully")

  //  create spark session
  val sparkSession:SparkSession = new SparkSessionHandler(sparkSessionSection).getSparkSession


  //Reading Stream: MURSVP
  val inputStreamMURSVP:DataFrame = new InputStreamHandler(sparkInputStreamMeetupRSVPSection,sparkSession).getInputStream
  val inputStreamMURSVPWithSchema:DataFrame = new ApplySchema(sparkSchemaMeetupRSVPSection, inputStreamMURSVP, sparkSession).getDFWithSchema

  //  Monitoring section
  val sparkMonitoringSection = ini.get("SparkMonitoring")


  //  Aggregate: Data
  val aggMURSVP = new Aggregate(inputStreamMURSVPWithSchema,sparkAggregationSection, sparkSchemaMeetupRSVPSection).getAggregatedDF

  val queryMURSVP =  new WriteStreamQuery(aggMURSVP, sparkWriteStreamSection, sparkSession, "MeetupRSVP").getWriteStream

  sparkSession.streams.awaitAnyTermination()
  queryMURSVP.stop()
}


