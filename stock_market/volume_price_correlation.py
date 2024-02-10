from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.appName("StockMarket").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# List of paths for American stocks stored on HDFS
paths = ['/user/s2112132/project_group25_data/Stocks_American/sp500/*.csv',
         '/user/s2112132/project_group25_data/Stocks_American/nasdaq/*.csv',
         '/user/s2112132/project_group25_data/Stocks_American/forbes2000/*.csv',
         '/user/s2112132/project_group25_data/Stocks_American/nyse/*.csv']


for path in paths:
    df = spark.read.option("header", "true").csv(path).select("Date", "Open", "Close", "Volume")
    
    # Set up the dataframe to perform calculations in the following steps.
    df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy").cast(DateType()))
    df = df.withColumn("Year", year(col("Date")))
    df = df.withColumn("FilePath", input_file_name())
    df = df.withColumn("Company", regexp_extract("FilePath", '([^/]+)\\.csv$', 1))
    df = df.drop("FilePath")
    windowSpec = Window.partitionBy("Company").orderBy("Date")
    windowSpec = Window.partitionBy("Year").orderBy(col("Yearly_Volume_Price_Corr").desc())

    # Calculate price fluctuation
    df = df.withColumn("Price Fluctuation", col("Close") - col("Open"))

    # Group by Company and Year, and calculate the correlation
    yearly_correlation_df = df.groupBy("Company", "Year").agg(round(corr("Volume", "Price Fluctuation"), 3).alias("Yearly_Volume_Price_Corr"))

    # Rank companies based on their correlation by year
    ranked_yearly_correlation = yearly_correlation_df.withColumn("Rank", rank().over(windowSpec))

    # Filter to get only the top 50 companies for each year and count the frequency of each company that appear in the top 50
    top50_yearly_correlation = ranked_yearly_correlation.filter(col("Rank") <= 50)
    company_appearances = top50_yearly_correlation.groupBy("Company").agg(count("*").alias("Appearances"),round(avg("Rank"), 3).alias("AverageRank"))

    # Get the top 10 companie that are ranked in the top 50 every year 
    top_companies_consistently = company_appearances.orderBy(col("Appearances").desc(), col("AverageRank")).limit(10)
    top_companies_consistently.show()
    top_companies = top_companies_consistently.select("Company").rdd.flatMap(lambda x: x).collect()

    # Join the original df with the yearly_correlation_df to get the Yearly_Volume_Price_Corr
    df_top10_annualized = df.join(yearly_correlation_df, ["Company", "Year"], "inner").filter(df.Company.isin(top_companies))
    df_top10_annualized_selected = df_top10_annualized \
        .groupBy("Year", "Company") \
        .agg(first("Yearly_Volume_Price_Corr").alias("Yearly_Volume_Price_Corr")) \
        .orderBy("Company", "Year")

    # Download file
    market_name = path.split('/')[-2]
    output_path = f'/user/s2261294/STOCK_MARKET_CORRELATION/{market_name}'
    df_top10_annualized_selected.write.csv(output_path, mode="ignore", header=True)
