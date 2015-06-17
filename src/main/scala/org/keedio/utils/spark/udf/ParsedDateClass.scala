package org.keedio.utils.spark.udf

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Map
import com.google.code.regexp.Pattern
import com.typesafe.config.ConfigFactory


/**
 * Created by J.David González "jdgonzalez@keedio.com" on 15/6/15.
 */

object REGEX {

  val configFactory = ConfigFactory.load("regex/regex.properties")

  def dataRegexPattern : Pattern = Pattern.compile(configFactory.getString("dateregex"))

  def dataRegexMap(inputbox : String) : Map[String, String] = {

    dataRegexPattern.matcher(inputbox).find()
    dataRegexPattern.matcher(inputbox).namedGroups()

  }

  def isSign(inputbox : String) : Boolean = {inputbox == "-"}
  def isNumeric(inputbox: String): Boolean = {if (inputbox != null) inputbox.forall(_.isDigit) else false}
  def isField(inputbox : String) : Boolean = List("d","h","m","s").contains(inputbox)

}

object UDF {

  def to_date(inputbox : String) : Timestamp = {

    val dateFormated = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    new Timestamp(dateFormated.parse(inputbox).getTime())

  }

  def to_code(fieldCase : String, numberCase : Int) : Timestamp = {

    val cal = java.util.Calendar.getInstance()

    fieldCase match {

      case "d" => cal.add(java.util.Calendar.DATE, -numberCase)
      case "h" => cal.add(java.util.Calendar.HOUR, -numberCase)
      case "m" => cal.add(java.util.Calendar.MINUTE, -numberCase)
      case "s" => cal.add(java.util.Calendar.SECOND, -numberCase)

    }

    new Timestamp(cal.getTimeInMillis)

  }

}

class ParsedDateClass {

  def parsedDate(inputbox : String) : Timestamp = {

    val mapREGEX = REGEX.dataRegexMap(inputbox)

    if (mapREGEX.get("now") != null)
      UDF.to_code("h",0)
    else if (mapREGEX.get("year") != null)
      UDF.to_date(inputbox)
    else if ( REGEX.isSign(mapREGEX.get("sign")) && REGEX.isNumeric(mapREGEX.get("number")) && REGEX.isField(mapREGEX.get("field")) )
      UDF.to_code(mapREGEX.get("field").toString, mapREGEX.get("number").toInt)
    else
      throw new IllegalArgumentException("\n\n -> Invalid timestamp: "+inputbox+".\n\n Expected format: \n 1) yyyy-MM-dd HH:mm \n 2) The last '-#@' where # is number and @ is d: day, h: hour, m: minute, s: second \n 3) now\n\n")
  }

}

object MainParsedDate extends ParsedDateClass{

  def main(args: Array[String]) {

    val dateString = "2015-06-01 23:01"
    val codeString = "-5h"
    val codeActualString = "-0h"
    val codeErrorString = "-h0"

    println()
    println("dateString:" + dateString + " -> " + parsedDate(dateString))
    println()
    println("codeString:" + codeString + " -> " + parsedDate(codeString))
    println()
    println("codeActualString:" + codeActualString + " -> " + parsedDate(codeActualString))
    println()
    println("now -> " + parsedDate("now"))
    println()
    println("codeErrorString:" + codeErrorString + " -> " + parsedDate(codeErrorString))

    System.exit(0)
  }
}
