* Running structured_network_wordcount.py

```
/usr/bin/spark-submit  --deploy-mode cluster --master yarn learn_pyspark/structured_network_wordcount.py
```
vs.
```
/usr/bin/spark-submit  --deploy-mode client --master yarn learn_pyspark/structured_network_wordcount.py
```
  * deploy-mode of client is right in this case since "console" output-mode is used in the code
  * this code is from https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets
