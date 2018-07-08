"""
Streaming ETL on CloudTrail Logs using Structured Streaming
In this Python notebook, we are going to explore how we can use Structured Streaming to perform streaming ETL on CloudTrail logs. For more context, read the Databricks blog.

AWS CloudTrail is a web service that records AWS API calls for your account and delivers audit logs to you as JSON files in a S3 bucket. If you do not have it configured, see their documentations on how to do so.
"""

# Step 1: Where is your input data and where do you want your final Parquet table?
# To run this notebook, first of all, you need to specify the location of the CloudTrail logs files. You can open your CloudTrail configuration, find the bucket and set the value below.

cloudTrailLogsPath = "s3n://MY_CLOUDTRAIL_BUCKET/AWSLogs/*/CloudTrail/*/2017/01/03/"
parquetOutputPath = "/MY_OUTPUT_PATH"  # DBFS or S3 path 

# Step 2: What is the schema of your data?
# To parse the JSON files, we need to know schema of the JSON data in the log files. Below is the schema defined based on the format defined in CloudTrail documentation. It is essentially an array (named Records) of fields related to events, some of which are nested structures.

from pyspark.sql.functions import *
from pyspark.sql.streaming import ProcessingTime
from pyspark.sql.types import *
from datetime import datetime

cloudTrailSchema = StructType() \
  .add("Records", ArrayType(StructType() \
    .add("additionalEventData", StringType()) \
    .add("apiVersion", StringType()) \
    .add("awsRegion", StringType()) \
    .add("errorCode", StringType()) \
    .add("errorMessage", StringType()) \
    .add("eventID", StringType()) \
    .add("eventName", StringType()) \
    .add("eventSource", StringType()) \
    .add("eventTime", StringType()) \
    .add("eventType", StringType()) \
    .add("eventVersion", StringType()) \
    .add("readOnly", BooleanType()) \
    .add("recipientAccountId", StringType()) \
    .add("requestID", StringType()) \
    .add("requestParameters", MapType(StringType(), StringType())) \
    .add("resources", ArrayType(StructType() \
      .add("ARN", StringType()) \
      .add("accountId", StringType()) \
      .add("type", StringType()) \
    )) \
    .add("responseElements", MapType(StringType(), StringType())) \
    .add("sharedEventID", StringType()) \
    .add("sourceIPAddress", StringType()) \
    .add("serviceEventDetails", MapType(StringType(), StringType())) \
    .add("userAgent", StringType()) \
    .add("userIdentity", StructType() \
      .add("accessKeyId", StringType()) \
      .add("accountId", StringType()) \
      .add("arn", StringType()) \
      .add("invokedBy", StringType()) \
      .add("principalId", StringType()) \
      .add("sessionContext", StructType() \
        .add("attributes", StructType() \
          .add("creationDate", StringType()) \
          .add("mfaAuthenticated", StringType()) \
        ) \
        .add("sessionIssuer", StructType() \
          .add("accountId", StringType()) \
          .add("arn", StringType()) \
          .add("principalId", StringType()) \
          .add("type", StringType()) \
          .add("userName", StringType()) \
        )
      ) \
      .add("type", StringType()) \
      .add("userName", StringType()) \
      .add("webIdFederationData", StructType() \
        .add("federatedProvider", StringType()) \
        .add("attributes", MapType(StringType(), StringType())) \
      )
    ) \
    .add("vpcEndpointId", StringType())))

# Step 3: Let's do streaming ETL on it!
# Now, we can start reading the data and writing to Parquet table. First, we are going to create the streaming DataFrame that represents the raw records in the files, using the schema we have defined. We are also option maxFilesPerTrigger to get earlier access the final Parquet data, as this limit the number of log files processed and written out every trigger.

rawRecords = spark.readStream \
  .option("maxFilesPerTrigger", "100") \
  .schema(cloudTrailSchema) \
  .json(cloudTrailLogsPath)
  
# Then, we are going to transform the data in the following way.
# 
# - Explode (split) the array of records loaded from each file into separate records.
# - Parse the string event time string in each record to Spark’s timestamp type.
# - Flatten out the nested columns for easier querying.

cloudTrailEvents = rawRecords \
  .select(explode("Records").alias("record")) \
  .select(
    unix_timestamp("record.eventTime", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp").alias("timestamp"),
    "record.*")

# Finally, we can define how to write out the transformed data and start the StreamingQuery. We are going to do the following
# 
# Write the data out in the Parquet format,
# Define the date column from that timestamp and partition the Parquet data by date for efficient time-slice queries.
# Define the trigger to be every 10 seconds.
# Define the checkpoint location
# Finally, start the query

checkpointPath = "/cloudtrail.checkpoint/

streamingETLQuery = cloudTrailEvents \
  .withColumn("date", cloudTrailEvents.timestamp.cast("date")) \
  .writeStream \
  .format("parquet") \
  .option("path", parquetOutputPath) \
  .partitionBy("date") \
  .trigger(processingTime="10 seconds") \
  .option("checkpointLocation", checkpointPath) \
  .start()
  
# Step 4: Query up-to-the-minute data from Parquet Table
# While the streamingETLQuery is continuously converting the data to Parquet, you can already start running ad-hoc queries on the Parquet table. Your queries will always pick up the latest written files while ensuring data consistency.

parquetData = sql("select * from parquet.`{}`".format(parquetOutputPath))
display(parquetData)

# If you count the number of rows in the table, you should find the value increasing over time. Run the following every few minutes.
sql("select * from parquet.`{}`".format(parquetOutputPath)).count()
