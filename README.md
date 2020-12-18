# MIRO: Event Log Processor

## Build
```bash
sbt build
```
## Run Event Log Parser
```bash
spark-submit \
--class com.miro.spark.userevents.LogParser \
./target/scala-2.12/miro-thuiswerk-assembly-0.0.1-SNAPSHOT.jar \
--overwrite true
```

## Run Event Stats Generator
```bash
spark-submit \
--class com.miro.spark.userevents.StatsGenerator \
./target/scala-2.12/miro-thuiswerk-assembly-0.0.1-SNAPSHOT.jar
```

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


