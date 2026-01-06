package com.yelpbatch.app
import com.yelpbatch.utils.DateUtils
import java.time.LocalDate
import java.time.temporal.ChronoUnit

case class JobArguments (
                          process: String,
                          env: String,
                          runDate: Seq[String],
                          startDate: Option[String],
                          endDate: Option[String],
                          tablesOpt: Option[String],
                          forceMonth: Option[String],
                          postGreSQLUser: Option[String],
                          postGreSQLPassword: Option[String],
                          skipDaily: Boolean,
                          dryRun: Boolean,
                          fullLoad: Boolean,
                          skipBronze: Boolean
  )
object JobArguments {
  private val datePattern: String = "yyyy-MM-dd"
  private val MaxRangeDays: Int = 31 // Safety cap to prevent accidental large ranges

  def parse(args: Array[String]): JobArguments = {

    @scala.annotation.tailrec
    def parseArgs(map: Map[String, String], list: List[String]): Map[String, String] = {
      list match {
        case Nil => map

        // Process
        case ("-p" | "--process") :: value :: tail =>
          parseArgs(map + ("process" -> value), tail)

        // Env
        case ("-e" | "--env") :: value :: tail =>
          parseArgs(map + ("env" -> value), tail)

        // Run Date
        case ("-d" | "--run_date" | "--runDate") :: value :: tail =>
          parseArgs(map + ("run_date" -> value), tail)

        // Start Date
        case ("-sd" | "--start_date" | "--startDate") :: value :: tail =>
          parseArgs(map + ("start_date" -> value), tail)

        // End Date
        case ("-ed" | "--end_date" | "--endDate") :: value :: tail =>
          parseArgs(map + ("end_date" -> value), tail)

        // Tables
        case ("-t" | "--tables") :: value :: tail =>
          parseArgs(map + ("tables" -> value), tail)

        // Force Monthly
        case ("-fm" | "--force_monthly") :: value :: tail =>
          parseArgs(map + ("force_monthly" -> value), tail)

        // PG User
        case ("-u" | "--pg_user") :: value :: tail =>
          parseArgs(map + ("pg_user" -> value), tail)

        // PG Password
        case ("-pw" | "--pg_password") :: value :: tail =>
          parseArgs(map + ("pg_password" -> value), tail)

        // Boolean flags (handle optional value "true"/"false" or implicit true)
        case (key @ ("--skip_daily" | "--dry_run" | "--full_load" | "--skip_bronze")) :: tail =>
          val normalizedKey = key.stripPrefix("--")
          tail match {
            case value :: rest if !value.startsWith("-") =>
              parseArgs(map + (normalizedKey -> value), rest)
            case _ =>
              parseArgs(map + (normalizedKey -> "true"), tail)
          }

        case option :: tail =>
          println(s"Unknown argument: $option")
          parseArgs(map, tail)
      }
    }

    val m = parseArgs(Map(), args.toList)

    // Extract arguments with defaults
    val process = m.getOrElse("process", throw new IllegalArgumentException("Missing required argument: --process or -p"))
    val env = m.getOrElse("env", throw new IllegalArgumentException("Missing required argument: --env or -e"))

    // Handle date arguments and check format "YYYY-MM-DD"
    val runDateOpt: Option[String] = m.get("run_date")
    val startOpt: Option[String] = m.get("start_date")
    val endOpt: Option[String] = m.get("end_date")


    val runDates: Seq[String] = (runDateOpt, startOpt, endOpt) match {
      // Case when only run_date is provided (single date)
      case (Some(date), None, None) =>
        // Validate date format
        val d = DateUtils.normalizeDate(d = date, datePattern = datePattern, field = "--run_date")
        Seq(d)
      // Date Range
      case (None, Some(start), Some(end)) =>
        // Validate date formats
        val sStr = DateUtils.normalizeDate(d = start, datePattern = datePattern, field = "--start_date")
        val eStr = DateUtils.normalizeDate(d = end, datePattern = datePattern, field = "--end_date")

        val startDate = LocalDate.parse(sStr)
        val endDate = LocalDate.parse(eStr)

        // Validate Logic
        if (endDate.isBefore(startDate)) {
          throw new IllegalArgumentException(s"end_date $end is before start_date $start")
        }
        // Validate Range Size
        val inclusiveDays = ChronoUnit.DAYS.between(startDate, endDate).toInt + 1 // inclusive
        if (inclusiveDays > MaxRangeDays) {
          throw new IllegalArgumentException(s"Date range exceeds maximum allowed of $MaxRangeDays days.")
        }
        // Generate date sequence
        DateUtils.dateRange(sStr, eStr)

     // Invalid combinations : All date args provided
      case (Some(_), Some(_), _) | (Some(_), _, Some(_)) =>
        throw new IllegalArgumentException("Ambiguous arguments: Cannot specify both --run_date and date range (--start_date/--end_date)")
      // Invalid combinations : Incomplete range
      case (None, Some(_), None) | (None, None, Some(_)) =>
        throw new IllegalArgumentException("Incomplete range: Both --start_date and --end_date are required")
      // Invalid combinations : No date args provided
      case (None, None, None) =>
        throw new IllegalArgumentException("Missing date arguments: Provide either --run_date (-d) or --start_date (-sd) & --end_date (-ed)")
    }

    // Helper for boolean flags
    def getBooleanFlag(key : String): Boolean =
      m.get(key).exists(_.toLowerCase == "true")

    // Helper to normalize tables argument
    def parseTables(tablesStr: String): Option[String] = {
      val trimmed: String = tablesStr.trim
      // remove surrounding brackets if present
      val inside: String = if (trimmed.startsWith("[") && trimmed.endsWith("]")) trimmed.substring(1, trimmed.length -1)
      else trimmed
      // Unify quotes and split by comma
      val items: Array[String] = inside.replace('\'', '"')
        .split(",")
        .map(_.trim)
        .map(_.stripPrefix("\"").stripSuffix("\""))
        .filter(_.nonEmpty)
      // Return None if empty
      if (items.isEmpty) None else Some(items.mkString(","))
    }

    JobArguments(
      process = process,
      env = env,
      runDate = runDates,
      startDate = startOpt,
      endDate = endOpt,
      tablesOpt = m.get("tables").flatMap(parseTables),
      forceMonth = m.get("force_monthly"),
      postGreSQLUser = m.get("pg_user"),
      postGreSQLPassword = m.get("pg_password"),
      skipDaily = getBooleanFlag("skip_daily"),
      dryRun = getBooleanFlag("dry_run"),
      fullLoad = getBooleanFlag("full_load"),
      skipBronze = getBooleanFlag("skip_bronze")
    )
  }
}