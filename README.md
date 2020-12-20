# MIRO: Event Log Processor

Event Log Processor is a package containing a couple Apache Spark applications
- [Event Log Parser](#event-log-parser)
- [Stats Generator](#stats-generator)

Please refer to [`DESIGN.md`](./DESIGN.md) for some notes on design considerations.

## Build

Make sure you have the following software versions installed in your environment:
- JDK 8
- Scala 2.12.10
- Apache Spark 3.0.1

The project is built as a standard Scala SBT project:

```bash
$ git clone https://github.com/HyunsungJo/miro-thuiswerk.git {project_root}
$ cd {project_root}
$ sbt assembly
```
A successful build should yield a fat JAR under `{project_root}/target/target/scala-2.12/miro-thuiswerk-assembly-0.0.1-SNAPSHOT.jar`.

## Event Log Parser

*Event Log Parser* ETL processor to parse and store raw client event logs - `registered` and `app_loaded` - as tables in Apache Parquet format.

### Input

By default the Event Log Parser will take sample data under `{project_root}/data/dataset.json` as input.

You can also specify you own dataset with an `--input-data` option where each row in the file reflects the following JSON format:

```json
{"event":"app_loaded", "timestamp":"2020-11-17T11:08:42.000Z", "initiator_id":3, "device_type":"mobile","browser_version":"18.18362","channel":"invite","campaign":""}
```

### Run

#### Command

Execute the following `spark-submit` command from your {project_root}:

```bash
$ spark-submit \
--class com.miro.spark.userevents.LogParser \
./target/scala-2.12/miro-thuiswerk-assembly-0.0.1-SNAPSHOT.jar \
--overwrite true
```

#### Options

Here are some supported options:

| Options                   | Description                                          | Example Values
|---------------------------|------------------------------------------------------|------------------------
| -s, --storage <value>     | local or remote storage                              | `"local"` or `"remote"`
| -i, --input-path <value>  | path of input JSON data                              | `"./data/dataset.json"`
| -o, --output-path <value> | path of output Parquet tables                        | `"./spark-warehouse"`
| -b, --bucket-size <value> | bucket size of output Parquet tables                 | 10
| -w, --overwrite <value>   | whether job should overwrite existing Parquet tables | `"false"`
| --help                    | prints usage text                                    |

### Output

Resulting Parquet output tables are stored under `{project_root}/spark-warehouse/`
with table names `registered` and `app_loaded` by default.

Use the `-o` or `--output-path` options to specify custom output paths.


## Stats Generator

*Stats Generator* reads from `registered` and `app_loaded` Parquet tables to generate a metric representing
the percentage of all users who loaded the application in the calendar week immediately after their registration week.

### Input

By default the Event Log Parser will take Parquet tables under `{project_root}/spark-warehouse/` as input.

You can also specify you own dataset with an `--table-path` option where table names are `registered` and `app_loaded`
in the format of Apache Parquet.

### Run

#### Command

Execute the following `spark-submit` command from your `{project_root}`:
```bash
$ spark-submit \
--class com.miro.spark.userevents.StatsGenerator \
./target/scala-2.12/miro-thuiswerk-assembly-0.0.1-SNAPSHOT.jar
```

#### Options

Here are some supported options:

| Options                   | Description                                          | Example Values
|---------------------------|------------------------------------------------------|------------------------
| -s, --storage <value>     | local or remote (WIP) storage                        | `"local"` or `"remote"`
| -t, --table-path <value>  | path of event `registered` and `app_loaded` tables   | `"./data/dataset.json"`
| -p, --period <value>      | period to generate the metric based on               | `"week"`, `"month"`, or `"year"`
| -w, --overwrite <value>   | whether job should overwrite existing Parquet tables | `"false"`
| --help                    | prints usage text                                    |

### Output

Result metric will be printed to console:

```bash
==========

target: 554.0 / total: 149990.0 = metric: 0.37 %

==========
```
Each terms represent the following:
- `target`: The distinct count of `initiator_id`s who triggered an `app_loaded` event the very calendar week (/month /year)
after the calendar week (/month /year) they `registered`
- `total`: The distinct count of `initiator_id`s in the input event log dataset
- `metric`: The rate of `target` over `total` in percent

## Development

Some development-related commands are as follows:

- Compile
```bash
$ sbt clean compile
```
- Unit test
```bash
$ sbt coverage test
```
- Code coverage
```bash
$ sbt coverageReport
```
- Code lint
```bash
$ sbt scalastyle
```
- Build fat JAR
```bash
$ sbt assembly
```
- Generate documents
```bash
$ sbt doc
```

