package ir.brgroup

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.DataStreamReader
import org.ini4j.Profile

import scala.collection.JavaConversions._
import scala.collection.mutable

class InputStreamHandler (inputStreamSection: Profile.Section, sparkSession:SparkSession) {
  val inputFormat: String = inputStreamSection.get("format",classOf[String])
  val readStream: DataStreamReader = sparkSession.readStream.format(inputFormat)
  val sparkReadStreamKeys: mutable.Set[String] = inputStreamSection.keySet().filter{ x =>  x !="format"}

  def getInputStream: DataFrame = {

    for (optionKey <- sparkReadStreamKeys) {
      readStream.option(optionKey, inputStreamSection.get(optionKey,classOf[String]))
    }
    readStream.load()
    //    streamData
  }
}