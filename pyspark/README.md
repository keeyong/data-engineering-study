
## What is Spark Structured Streaming?

### Spark Structured Streaming Overview

* Unifies streaming, interactive and batch queries - a single API for both static bounded data and streaming unbounded data
* Runs on Spark SQL. Uses the Spark SQL Dataset/DataFrame API used for batch processing of static data
* Runs incrementally and continuously and updates the results as data streams in
* Supports app development in Scala, Java, Python and R
* Supports streaming aggregations, event-time windows, windowed grouped aggregation, stream-to-batch joins
* Features streaming deduplication, multiple output modes and APIs for managing/monitoring streaming queries
* Built-in sources: Kafka, File source (json, csv, text, parquet)
* Structured Streaming supports a ProcessingTime trigger which will fire every user-provided interval, for example every minute (https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html)

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
* Spark SQL supports complex types natively from https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html
  * Selecting from nested columns: events.select("a.b")
  * Flattening structs: events.select("a.*")
  * Nesting columns
  * Nesting all columns
  * Selecting a single array or map element
  * Creating a row for each array or map element
  * Collecting multiple rows into an array
  * Selecting one field from each item in an array
  * to_json and from_json
  * Parse a set of fields from a column containing JSON
  * Parse a well-formed string column
  
* Real-time Streaming ETL with Structured Streaming in Apache Spark
  * it is easy to take an existing batch ETL job and subsequently productize it as a real-time streaming pipeline using Structured Streaming
```
  // Read data continuously from an S3 location
val inputDF = spark.readStream.json("s3://logs")
 
// Do operations using the standard DataFrame API and write to MySQL
inputDF.groupBy($"action", window($"time", "1 hour")).count()
       .writeStream.format("jdbc")
       .start("jdbc:mysql//…")
```
This code is nearly identical to the batch version below—only the “read” and “write” changed:
```
// Read data once from an S3 location
val inputDF = spark.read.json("s3://logs")
 
// Do operations using the standard DataFrame API and write to MySQL
inputDF.groupBy($"action", window($"time", "1 hour")).count()
       .writeStream.format("jdbc")
       .save("jdbc:mysql//…")
```       
  * ETL needs to do the followings:
    * Filter, transform, and clean up data
    * Convert to a more efficient storage format
      * Text, JSON and CSV data are easy to generate and are human readable, but are very expensive to query. Converting it to more efficient formats like Parquet, Avro, or ORC can reduce file size and improve processing speed.
    * Partition data by important columns
      * By partitioning the data based on the value of one or more columns, common queries can be answered more efficiently by reading only the relevant fraction of the total dataset.
  * Fortunately, Structured Streaming makes it easy to convert these periodic batch jobs to a real-time data pipeline. Streaming jobs are expressed using the same APIs as batch data. Additionally, the engine provides the same fault-tolerance and data consistency guarantees as periodic batch jobs, while providing much lower end-to-end latency.

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
