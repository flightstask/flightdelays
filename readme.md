# Flight Data ETL

## Download data
To download data run:

```bash
./gradlew getData
```

No need to unzip CSV files Spark works with bzipped files just fine. On the other hand, our data contains a lot of collumns which we will not use. Thus, converting it to Parquet will give us an order of magnitute speedup. Beware, it will take some time to convert the data localy. Conversion can be done with the following command localy:

```bash
./gradlew convertToParquet
```

Or on a spark cluster using spark-submit script provided by Spark distribution. For example to submit to YARN based cluster run:

```bash
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --class ConvertToParquet build/libs/plains.jar /path/to/input/csv/files /output/path
```

## ETL tasks

In order to find the busiest airports run the following command:

```bash
./gradlew busyAirports
```

It should print TOP20 busiest airports to console and write output in CSV format to ./output/1_busyAirports.csv file.

To find flights you should avoid (with highest number of delays) run:

```bash
./gradlew worstDelayRoutes
```

Since absolute numbers can be missleading (the bigger the airport the more delays), delay ratio normalised by total flights is also included in the result.

