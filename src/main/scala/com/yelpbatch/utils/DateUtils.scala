package com.yelpbatch.utils

import java.time.{LocalDate, YearMonth}

object DateUtils {
  /** Validate/normalize YYYY-MM-DD */
  def normalizeDate(d: String): String = LocalDate.parse(d).toString

  /** Is YYYY-MM-DD the last day of its month? */
  def isEndOfMonthStr(d: String): Boolean = {
    val ld = LocalDate.parse(d)
    ld == YearMonth.from(ld).atEndOfMonth()
  }

  /** "YYYY-MM-DD" -> "YYYY-MM" */
  def toYearMonth(d: String): String = YearMonth.from(LocalDate.parse(d)).toString

  /** "YYYY-MM" -> ("YYYY-MM-01", "YYYY-MM-<eom>") */
  def monthWindow(ym: String): (String, String) = {
    val y = YearMonth.parse(ym)
    (y.atDay(1).toString, y.atEndOfMonth().toString)
  }

  /** Iterate days inclusive: start<=d<=end (both "YYYY-MM-DD") */
  def dateRange(start: String, end: String): Seq[String] = {
    val s = LocalDate.parse(start); val e = LocalDate.parse(end)
    Iterator.iterate(s)(_.plusDays(1)).takeWhile(!_.isAfter(e)).map(_.toString).toSeq
  }

  def toMonthStart(d: String): String = {
    val ld = LocalDate.parse(d)
    ld.withDayOfMonth(1).toString
  }

  def toMonthEnd(d: String): String = {
    val ld = LocalDate.parse(d)
    ld.withDayOfMonth(ld.lengthOfMonth()).toString
  }
}