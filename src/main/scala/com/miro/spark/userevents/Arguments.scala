package com.miro.spark.userevents

import scopt.OParser

trait Arguments {
  def storage: String
}

case class LogParserArguments(
  storage: String = "local",
  inputPath: String = "./data/dataset.json",
  outputPath: String = "./spark-warehouse",
  bucketSize: Int = 10,
  overwrite: Boolean = false
) extends Arguments

case class StatsGeneratorArguments(
  storage: String = "local",
  tablePath: String = "./spark-warehouse",
  period: String = "week"
) extends Arguments

class ArgumentsParser {
  private val logParserArgumentsBuilder = OParser.builder[LogParserArguments]
  val logParserArgumentsParser = {
    import logParserArgumentsBuilder._
    OParser.sequence(
      programName("user-events: log-parser"),
      head("scopt", "4.x"),

      opt[String]('s', "storage")
        .action((x, c) => c.copy(storage = x))
        .text("local or remote storage"),

      opt[String]('i', "input-path")
        .action((x, c) => c.copy(inputPath = x))
        .text("path of input JSON data"),

      opt[String]('o', "output-path")
        .action((x, c) => c.copy(outputPath = x))
        .text("path of output Parquet tables"),

      opt[Int]('b', "bucket-size")
        .action((x, c) => c.copy(bucketSize = x))
        .validate(x =>
          if (0 < x && x <= 100) success
          else failure("Value <bucket-size> must be in (0, 100]"))
        .text("bucket size of output Parquet tables"),

      opt[Boolean]('w', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("whether job should overwrite output Parquet tables")
    )
  }

  private val statsGeneratorArgumentsBuilder = OParser.builder[StatsGeneratorArguments]

  val statsGeneratorArgumentsParser = {
    import statsGeneratorArgumentsBuilder._
    OParser.sequence(
      programName("user-events: stats-generator"),
      head("scopt", "4.x"),
      opt[String]('s', "storage")
        .action((x, c) => c.copy(storage = x))
        .text("local or remote storage"),
      opt[String]('t', "table-path")
        .action((x, c) => c.copy(tablePath = x))
        .text("path of event Parquet tables"),
      opt[String]('p', "period")
        .action((x, c) => c.copy(period = x))
        .validate(x =>
          if (Seq("week", "month", "year").contains(x)) success
          else failure("Possible values of <period> are 'week', 'month', or 'year'"))
        .text("period to watch conversion")
    )
  }
}

