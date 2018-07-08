
## What is Spark Structured Streaming?

### Spark Structured Streaming Overview

* Unifies streaming, interactive and batch queries - a single API for both static bounded data and streaming unbounded data
* Runs on Spark SQL. Uses the Spark SQL Dataset/DataFrame API used for batch processing of static data
* Runs incrementally and continuously and updates the results as data streams in
* Supports app development in Scala, Java, Python and R
* Supports streaming aggregations, event-time windows, windowed grouped aggregation, stream-to-batch joins
* Features streaming deduplication, multiple output modes and APIs for managing/monitoring streaming queries
* Built-in sources: Kafka, File source (json, csv, text, parquet)

### Continuous Application

![Step 0](./imgs/cont_app.png)

### Parquet Format

* Transforming logs from JSON into Parquet shortened the runtime of our ad-hoc queries by 10x
* Spark SQL allows users to ingest data from these classes of data sources, both in batch and streaming queries. It natively supports reading and writing data in Parquet, ORC, JSON, CSV, and text format and a plethora of other connectors exist on Spark Package

```
events = spark.readStream \
  .format("json") \           # or parquet, kafka, orc...
  .option() \                 # format specific options
  .schema(my_schema) \        # required
  .load("path/to/data")

output = …                   # perform your transformations

output.writeStream \          # write out your data 
  .format("parquet") \
  .start("path/to/write")
```
* Spark SQL supports complex types natively
  * Selecting from nested columns: events.select("a.b")
  * Flattening structs: events.select("a.*")
   

## Example Scripts Overview

### Running structured_network_wordcount.py

```
/usr/bin/spark-submit  --deploy-mode cluster --master yarn learn_pyspark/structured_network_wordcount.py
```
vs.
```
/usr/bin/spark-submit  --deploy-mode client --master yarn learn_pyspark/structured_network_wordcount.py
```
  * deploy-mode of client is right in this case since "console" output-mode is used in the code
  * this code is from https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets
