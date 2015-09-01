package org.keedio.utils.spark.udf

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Map
import com.google.code.regexp.Pattern
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart, SparkListener}
import org.apache.spark.sql.SQLContext
import scala.util.DynamicVariable


/**
 * Created by J.David González "jdgonzalez@keedio.com" on 15/6/15.
 */

object REGEX {

  // DynamicVariables provide a binding mechanism where the current value is found through dynamic scope, but where access to the variable itself is resolved through static scope.
  val dataRegexPattern: Pattern = Pattern.compile("(((?<year>[0-9]{1,4})(/|-)(?<month>[0-9]{1,2})(/|-)(?<day>[0-9]{1,2})) ((?<hour>[0-9]{1,2}):(?<minute>[0-9]{1,2})))|((?<sign>(-|(\\+))?)(?<number>[0-9]+)(?<field>(s|m|h|d|M|y)))|((?<now>(now|^$)))")
  val dv = new DynamicVariable[Pattern](dataRegexPattern)

  /**
   *
   * @param inputbox -> Date with predefined format
   * @return -> Map of parsed date
   */
  def dataRegexMap(inputbox: String): Map[String, String] = {

    dv.value.matcher(inputbox).find()
    dv.value.matcher(inputbox).namedGroups()

  }

  /**
   * Check whether various fields of regex is the default setting
   *
   * @param inputbox
   * @return Boolean
   */
  def isSign(inputbox: String): Boolean = {
    if (inputbox != null) true else false
  }

  def isNumeric(inputbox: String): Boolean = {
    if (inputbox != null) inputbox.forall(_.isDigit) else false
  }

  def isField(inputbox: String): Boolean = List("y", "M", "d", "h", "m", "s").contains(inputbox)

}

case class UDFListener() extends SparkListener with Logging{
  log.info("Initializing UDFListener")

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    log.info(s"UDFListener onJobEnd: ${jobEnd.jobId}")
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    log.info(s"UDFListener onJobStart: ${jobStart.jobId}")
    UDF.parsedDateCache.clear
  }
}

object UDF {

  val logger = Logger.getLogger(this.getClass)

  val dateFormated = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  /**
   * parsed date cache
   */
  val parsedDateCache : scala.collection.mutable.Map[String, Timestamp] =
    scala.collection.mutable.Map[String, Timestamp]()


  /**
   * Register the functions to use in SQL's context
   *
   * @param sqlc
   */
  def registerUDF(sqlc: SQLContext): Unit = {
    logger.info("registering UDFs")
    sqlc.udf.register[String,String,String,String]("concat", concat)
    sqlc.udf.register("to_date", to_date _)
    sqlc.udf.register("to_hour", to_hour _)
    sqlc.udf.register("aggregationDay", aggregationDay _)
    sqlc.udf.register("to_code", to_code _)
    sqlc.udf.register("parseDate", parseDate _)

    sqlc.sparkContext.addSparkListener(UDFListener())
  }

  /**
   * This function concatenates a set of string.
   *
   * @return Concatenated string
   */
  def concat(separator: String, e1: String = "", e2: String = ""): String = {
    val elems = List(e1,e2)

    elems. filter (_.nonEmpty) mkString separator

  }

  /**
   *
   * @param inputbox -> input's date as a String
   * @return
   */
  def to_date(inputbox: String): Timestamp = {

    new Timestamp(dateFormated.parse(inputbox).getTime())

  }

  /**
   *
   * @param inputbox
   * @return String with the hour 'HH'
   */
  def to_hour(inputbox: Timestamp) : String = {

    REGEX.dataRegexMap(inputbox.toString).get("hour").toString

  }

  /**
   * Only the date of Timestamp
   *
   * @param inputbox
   * @return Date with format 'yyyy-MM-dd'
   */
  def aggregationDay(inputbox: Timestamp): String = {

    val mapR = REGEX.dataRegexMap(inputbox.toString)

    concat("-",
      mapR.get("year").toString,
      concat("-",
      mapR.get("month").toString,
      mapR.get("day").toString)
    )

  }

  /**
   *
   * Function to convert the coding in a timestamp
   *
   * @param fieldCase -> y: year, M: month, d: day, h: hour, m: minute, s: second
   * @param numberCase -> amount on measure of fieldCase
   * @return Timestamp
   */
  def to_code(fieldCase: String, numberCase: Int): Timestamp = {
    logger.info(s"to_code: $fieldCase, $numberCase")
    val cal = java.util.Calendar.getInstance()

    fieldCase match {

      case "y" => cal.add(java.util.Calendar.YEAR, numberCase)
      case "M" => cal.add(java.util.Calendar.MONTH, numberCase)
      case "d" => cal.add(java.util.Calendar.DATE, numberCase)
      case "h" => cal.add(java.util.Calendar.HOUR, numberCase)
      case "m" => cal.add(java.util.Calendar.MINUTE, numberCase)
      case "s" => cal.add(java.util.Calendar.SECOND, numberCase)

    }

    new Timestamp(cal.getTimeInMillis)

  }


  /**
   * This function parsed the date or codification to into
   * The input are parsed using Regular Expression. The target name are:
   *
   * 1) Date: 'year', 'month', 'day', 'hour', 'minute'
   *
   * 2) Code: 'sign' -> '-' or '+'
   * 'number'
   * 'field' -> y: year, M: month, d: day, h: hour, m: minute, s: second
   *
   * 3) Keyword: 'now'
   *
   * @param inputbox
   * @return Timestamp
   */
  def parseDate(inputbox: String): Timestamp = {
    if (parsedDateCache.contains(inputbox)){
      parsedDateCache(inputbox)
    } else {

      val mapREGEX = REGEX.dataRegexMap(inputbox)
      val sign = mapREGEX.get("sign")

      val ts = if ( inputbox == null || inputbox == "")
        UDF.to_code("h", 0)
      else if ( mapREGEX.get("now") != null )
        UDF.to_code("h", 0)
      else if ( mapREGEX.get("year") != null )
        UDF.to_date(inputbox)
      else if ( REGEX.isSign(mapREGEX.get("sign")) && REGEX.isNumeric(mapREGEX.get("number")) && REGEX.isField(mapREGEX.get("field")))
        UDF.to_code(mapREGEX.get("field").toString, (sign.concat(mapREGEX.get("number"))).toInt )
      else
        throw new IllegalArgumentException("\n\n -> Invalid timestamp: " + inputbox + ".\n" +
          "Expected format: \n" +
          "\\n 1) Date: 'yyyy-MM-dd HH:mm'" +
          "\\n 2) Code: It's necessary sign, number and letter {y: year, M: month, d: day, h: hour, m: minute, s: second}. Example: '-24h' are the last 24 hours from now" +
          "\\n 3) Keyword: 'now'\n\n")

      parsedDateCache.put(inputbox, ts)

      ts
    }
  }
}