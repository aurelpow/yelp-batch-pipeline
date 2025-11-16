package com.yelpbatch.utils

import java.time.{LocalDate, YearMonth}

object DateUtils {

  /**
   * Parse and return a normalized ISO date string for the supplied input.
   *
   * The method parses `d` using `LocalDate.parse` and returns the canonical
   * `YYYY-MM-DD` representation (same as `LocalDate.toString`).
   *
   * @param d date string in ISO format (`YYYY-MM-DD`)
   * @return normalized date string `YYYY-MM-DD`
   * @throws java.time.format.DateTimeParseException if `d` is not a valid ISO date
   */
  def normalizeDate(d: String): String = LocalDate.parse(d).toString

  /**
   * Determine whether the supplied date is the last day of its month.
   *
   * Example:
   * - "2024-02-29" => true (leap year)
   * - "2024-02-28" => false
   *
   * @param d date string in ISO format (`YYYY-MM-DD`)
   * @return true if `d` equals the month's end day, false otherwise
   * @throws java.time.format.DateTimeParseException if `d` is not a valid ISO date
   */
  def isEndOfMonthStr(d: String): Boolean = {
    val ld = LocalDate.parse(d)
    ld == YearMonth.from(ld).atEndOfMonth()
  }

  /**
   * Convert an ISO date string to its year-month component.
   *
   * Example:
   * - "2024-02-29" -> "2024-02"
   *
   * @param d date string in ISO format (`YYYY-MM-DD`)
   * @return year-month string `YYYY-MM`
   * @throws java.time.format.DateTimeParseException if `d` is not a valid ISO date
   */
  def toYearMonth(d: String): String = YearMonth.from(LocalDate.parse(d)).toString

  /**
   * Given a year-month string, return the month window as the first and last day.
   *
   * Example:
   * - "2024-02" -> ("2024-02-01", "2024-02-29")
   *
   * @param ym year-month string in ISO format (`YYYY-MM`)
   * @return tuple of (`YYYY-MM-01`, `YYYY-MM-<last-day>`)
   * @throws java.time.format.DateTimeParseException if `ym` is not a valid year-month
   */
  def monthWindow(ym: String): (String, String) = {
    val y = YearMonth.parse(ym)
    (y.atDay(1).toString, y.atEndOfMonth().toString)
  }

  /**
   * Iterate days inclusively from `start` to `end`.
   *
   * Produces a sequence of ISO date strings such that every element `d` satisfies
   * `start <= d <= end`. If `start` is after `end`, an empty sequence is returned.
   *
   * Example:
   * - dateRange("2024-02-27", "2024-03-01") ->
   *   Seq("2024-02-27", "2024-02-28", "2024-02-29", "2024-03-01")
   *
   * @param start inclusive start date `YYYY-MM-DD`
   * @param end inclusive end date `YYYY-MM-DD`
   * @return sequence of date strings from start to end (inclusive)
   * @throws java.time.format.DateTimeParseException if either input is invalid
   */
  def dateRange(start: String, end: String): Seq[String] = {
    val s = LocalDate.parse(start); val e = LocalDate.parse(end)
    Iterator.iterate(s)(_.plusDays(1)).takeWhile(!_.isAfter(e)).map(_.toString).toSeq
  }

  /**
   * Return the first day of the month for the given date.
   *
   * Example:
   * - "2024-02-15" -> "2024-02-01"
   *
   * @param d date string `YYYY-MM-DD`
   * @return date string representing the first day of that month
   * @throws java.time.format.DateTimeParseException if `d` is not valid
   */
  def toMonthStart(d: String): String = {
    val ld = LocalDate.parse(d)
    ld.withDayOfMonth(1).toString
  }

  /**
   * Return the last day of the month for the given date.
   *
   * Example:
   * - "2024-02-15" -> "2024-02-29"
   *
   * @param d date string `YYYY-MM-DD`
   * @return date string representing the last day of that month
   * @throws java.time.format.DateTimeParseException if `d` is not valid
   */
  def toMonthEnd(d: String): String = {
    val ld = LocalDate.parse(d)
    ld.withDayOfMonth(ld.lengthOfMonth()).toString
  }
}