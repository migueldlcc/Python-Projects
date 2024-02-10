from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder.appName("StockMarketVolatility").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# List of paths for both American and Indian stocks stored on HDFS
paths = ['/user/s2112132/project_group25_data/Stocks_American/sp500/*.csv',
         '/user/s2112132/project_group25_data/Stocks_American/nasdaq/*.csv',
         '/user/s2112132/project_group25_data/Stocks_American/forbes2000/*.csv',
         '/user/s2112132/project_group25_data/Stocks_American/nyse/*.csv',
         '/user/s2112132/project_group25_data/Stocks_Indian/*.csv']

# Define the initial schema with just the "Year" column
schema = StructType([StructField("Year", IntegerType(), False)])

# Initialize an empty DataFrame with the year column
main_df = spark.createDataFrame([], schema)

# Function to process each path
def process_path(path):
    # The schema of the Indian stock market data differs from the rest. Therefore we needed to do 2 different reading approaches are needed.
    if 'Stocks_Indian' in path:
        df = spark.read.option("header", "true").csv(path)
        df = df.select(col("timestamp").alias("Date"), col("close").alias("Adjusted Close"))
        df = df.withColumn("Date", to_timestamp("Date", "yyyy-MM-dd HH:mm:ssXXX"))
        df = df.filter(year(col("Date")) != 2021)
        name = "Indian Stock Market"
    else:
        df = spark.read.option("header", "true").csv(path).select("Date", "Adjusted Close")
        df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))
        name = path.split('/')[-2]

    # Set up the dataframe to perform calculations in the following steps.
    df = df.withColumn("Year", year(col("Date")))
    df = df.withColumn("FilePath", input_file_name())
    df = df.withColumn("Company", regexp_extract("FilePath", '([^/]+)\\.csv$', 1))
    df = df.drop("FilePath")

    # Calculate the Daily Return
    windowSpec = Window.partitionBy("Company").orderBy("Date")
    df = df.withColumn("Previous Close", lag("Adjusted Close").over(windowSpec))
    df = df.withColumn("Daily Return", when(col("Previous Close").isNull(), None)
                        .otherwise((col("Adjusted Close") - col("Previous Close")) / col("Previous Close")))

    # Calculate the Average Daily Return, Squared Difference and Volatility
    windowSpecYearly = Window.partitionBy("Company", "Year")
    df = df.withColumn("Average Daily Return", avg("Daily Return").over(windowSpecYearly))
    df = df.withColumn("Squared Difference", (col("Daily Return") - col("Average Daily Return")) ** 2)
    df = df.withColumn("Volatility", sqrt(sum("Squared Difference").over(windowSpecYearly) / (count("Daily Return").over(windowSpecYearly))))

    # Group by "Year" and calculate the average "Volatility"
    df_yearly_volatility = df.groupBy("Year").agg(round(avg("Volatility") * 100, 2).alias(f"{name}"))
    return df_yearly_volatility

# Looping through all paths and join the resulting dataframes into the main dataframe
for path in paths:
    df_yearly_volatility = process_path(path)
    if main_df.rdd.isEmpty():
        main_df = df_yearly_volatility
    else:
        main_df = main_df.join(df_yearly_volatility, on="Year", how="outer")

# Create file and save in the specified HDFS path
output_path = '/user/s2112132/project_group25_results/Volatility'
main_df.orderBy("Year").write.csv(output_path, mode="overwrite", header=True)
