# Design Notes
Some key design considerations for main objects and classes are as follows.

## SparkJob
- Common Spark settings method definitions for each app to share and implement.

## LogParser
- Application for event log parsing and storing as tables
- Splitting event types partitioning
- Caching to preserve and reuse split partitioned data
- Sorted/ partitioned/ tables for general purpose use of the result

## StatsGenerator
- Application for metric generation
- Logic to find target events:
  - Add *join column*s to both `registered` and `app_loaded` datasets.
  - Join column values are formatted as `"YYYY-{week_number}"`.
  - `registered` populates join column based on `time` + 1 week.
  - `app_laded` populates join column based on `time`.
  - [ASK] Repartition both dataset on the join column.
  - Joining two tables on same `initiator_id` + `join_key` will give us the target events.
  - No broadcast join assuming that both datasets are very large.

## EventStorage
- Abstraction layer responsible for managing reading and writing of data
- Defines schema of each Dataset
- Extendable to different log stores

## Arguments
- Commandline argument parsing and validation
- User-friendly `--help` option

## Other Project Setup
- Tests
  - Shared spark session for mocking
  - Unit tests
  - Test coverage
  - Code styles
- CI/ CD via GitHub Actions

## Dependencies

- `scala v2.12.10`
- `spark v3.0.1`
- `sbt v1.1.1` for building
- `sbt-assembly` for packaging
- `scopt v4.0.0` for argument parsing
- `scalatest v3.2.3` for testing
- `sbt-scoverage v1.6.1` for code coverage
- `scalastyle-sbt-plugin v1.0.0` for code linting
- `sbt-github-packages v0.5.2` for GitHub publishing

## TODO
- README
- launcher application
- data cleansing/ validation
  - is register event unique per user?
  - are timestamps unique?
  - does register event exist for every app load user?
  - validation should happen after initial filtering / partitioning
  - timestamp format
- checkpointing
- clean up
- tests
- paths
- constants
- optimizations (performance tuning)
- case class types
- variable types
- edge cases
  - empty dataframes, etc.
- parameters
  - path
  - date range
  - bucket size
  - clean up
  - overwrite
- deployment (kubernetes?)
- gitlab-ci
- comments

## DISCUSS
- filter vs intermediate table
  - if target data (event === "registered" || event === "app_loaded") can fit memory, filtering makes sense
  - if there are many other event types and target data is quite large, intermediate tables makes sense
- partitionBy vs repartition
- partition vs bucket
- in streaming
- hyper-log-log
- rdd vs datafram vs dataset
- cache & persist vs checkpointing

## FEEDBACK
- Timestamp format is different in the dataset vs pdf example


