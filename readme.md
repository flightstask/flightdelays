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

To calculate flight delay probabilities split by departure time block and day off week run:

```bash
./gradlew delayProbability
```

Where exists routes in the dataset with only one or few fligths within specified time window. In such cases point estimate of delay probability is not relevant. In practice, calculating confidence interval for our probability estimate would help. As a shortcut, I've added optional parameter for this task to specify minimum flight count per group which can be invoked like this:

```bash
./gradlew delayProbability -PminCount=100
```

Where were 3452 instances of departure time > 2400 for consistency of data we have removed those as well as rows with unknown arrival delay, departure time or day of week.

All of the jobs above can be run on a cluster environment with the help of spark-submit script.

## Tests

Tests can be run with the usual gradle command:

```bash
./gradlew test
```
