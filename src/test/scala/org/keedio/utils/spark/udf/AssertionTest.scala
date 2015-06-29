package org.keedio.utils.spark.udf

import java.sql.Timestamp

import org.junit.{Assert, Test}

/**
 * Created by J.David González "jdgonzalez@keedio.com" on 29/6/15.
 */
class AssertionTest {

  //REGEX test
  @Test
  def dataRegexMap$Test() = {

    val dateMap1 = REGEX.dataRegexMap("2015-01-01 00:01")
    val dateMap2 = REGEX.dataRegexMap("-24h")
    val dateMap3 = REGEX.dataRegexMap("now")

    Assert.assertNotNull("\n1st sample\n", dateMap1)
    Assert.assertEquals("\nYear!\n", dateMap1.get("year"),"2015")
    Assert.assertEquals("\nMonth!\n", dateMap1.get("month"),"01")
    Assert.assertEquals("\nDay!\n", dateMap1.get("day"),"01")
    Assert.assertEquals("\nHour!\n", dateMap1.get("hour"),"00")
    Assert.assertEquals("\nMinute!\n", dateMap1.get("minute"),"01")

    Assert.assertNotNull("\n2nd sample\n", dateMap2)
    Assert.assertEquals("\nSign!\n", dateMap2.get("sign"),"-")
    Assert.assertEquals("\nNumber!\n", dateMap2.get("number"),"24")
    Assert.assertEquals("\nField!\n", dateMap2.get("field"),"h")

    Assert.assertNotNull("\n3rd sample\n", dateMap3)
    Assert.assertEquals("\nCode!\n", dateMap3.get("now"),"now")

  }

  //UDF test
  @Test
  def concat$Test() = {

    val s : String = "s-1-2-t"

    Assert.assertEquals(s, UDF.concat("-","s",UDF.concat("-","2","t")))
    Assert.assertTrue("\n\nString length is not the correct: "+ s.length +" \n\n", s.length == 7)

  }

  @Test
  def to_date$Test() = {

    val s : String = "2015-01-01 00:01:01.001"
    val r : String = "2015-01-01 00:01:00.0"

    Assert.assertEquals(r,UDF.to_date(s).toString)

  }

  @Test
  def to_hour$Test() = {

    val s : String = "2015-01-01 03:01:01.001"
    val r : String = "03"

    Assert.assertEquals(r,UDF.to_hour(Timestamp.valueOf(s)).toString)

  }

  @Test
  def aggregationDay$Test() = {

    val s : String = "2015-01-01 00:01:01.001"

    Assert.assertEquals("2015-01-01", UDF.aggregationDay(Timestamp.valueOf(s)))
    Assert.assertNotEquals("2015-01-02", UDF.aggregationDay(Timestamp.valueOf(s)))

  }

  @Test
  def to_code$Test() = {

    val cal = java.util.Calendar.getInstance()

    Assert.assertEquals("ERROR year",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("y",0).toString))
    Assert.assertEquals("ERROR month",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("M",0).toString))
    Assert.assertEquals("ERROR day",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("d",0).toString))
    Assert.assertEquals("ERROR hour",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("h",0).toString))
    Assert.assertEquals("ERROR minute",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("m",0).toString))
    Assert.assertEquals("ERROR second",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("s",0).toString))

    Assert.assertNotEquals("no ERROR year",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("y",1).toString))
    Assert.assertNotEquals("no ERROR month",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("M",1).toString))
    Assert.assertNotEquals("no ERROR day",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("d",1).toString))
    Assert.assertNotEquals("no ERROR hour",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("h",1).toString))
    Assert.assertNotEquals("no ERROR minute",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.to_code("m",1).toString))

  }

  @Test
  def parseDate$Test() = {

    val cal = java.util.Calendar.getInstance()

    Assert.assertEquals("ERROR",UDF.to_date("2015-01-01 00:01:00.0"), UDF.to_date(UDF.parseDate("2015-01-01 00:01:01.001").toString))
    Assert.assertEquals("ERROR",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.parseDate("-0h").toString))
    Assert.assertEquals("ERROR",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.parseDate("-1s").toString))
    Assert.assertEquals("ERROR",UDF.to_date(new Timestamp(cal.getTimeInMillis).toString), UDF.to_date(UDF.parseDate("now").toString))

  }

}
