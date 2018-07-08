
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
