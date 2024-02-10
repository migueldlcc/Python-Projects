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
    df = spark.read.option("header", "true").csv(path).select("Date", "Adjusted Close")

    # Set up the dataframe to perform calculations in the following steps.
    df = df.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy").cast(DateType()))
    df = df.withColumn("Year", year(col("Date")))
    df = df.withColumn("FilePath", input_file_name())
    df = df.withColumn("Company", regexp_extract("FilePath", '([^/]+)\\.csv$', 1))
    df = df.drop("FilePath")
    windowSpec = Window.partitionBy("Company", "Year").orderBy("Date")
    windowSpecUnbounded = Window.partitionBy("Company", "Year").orderBy("Date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    windowSpecCompany = Window.partitionBy("Company")

    # Determine the first year of trading for each company
    df = df.withColumn("FirstYear", min(col("Year")).over(windowSpecCompany))

    # Get the first and last adjusted close price for each company for each year, and calculate the annual return as a percentage
    df = df.withColumn("FirstAdjustedClose", first(col("Adjusted Close")).over(windowSpecUnbounded))
    df = df.withColumn("LastAdjustedClose", last(col("Adjusted Close")).over(windowSpecUnbounded))
    df = df.withColumn("AnnualReturn", round((col("LastAdjustedClose") - col("FirstAdjustedClose")) / col("FirstAdjustedClose"), 3))

    # Now that there are multiple rows with the same annual return for each company and year, I keep the columns we need and drop the duplicates
    df_annual_returns = df.select("Company", "Year", "AnnualReturn").distinct()

    # Rank the companies based on AnnualReturn for each year
    windowSpec = Window.partitionBy("Year").orderBy(col("AnnualReturn").desc())
    df_annual = df_annual_returns.withColumn("Rank", rank().over(windowSpec))
    
    # Filter to get only the top 50 companies for each year and count the frequency of each company that appear in the top 50
    top50_each_year = df_annual.filter(col("Rank") <= 50)
    company_frequencies = top50_each_year.groupBy("Company").agg(count("*").alias("Appearances"), round(avg("Rank"), 3).alias("AverageRank"))

    # Get the top 10 companies that appear ranked on the top 50 the most times every year
    top_10_companies = company_frequencies.orderBy(col("Appearances").desc(), col("AverageRank")).limit(10)
    top_10_companies.show()

    # Select the distinct companies from the top 10 companies (convert the DataFrame to an RDD)
    top_companies_list = top_10_companies.select("Company").distinct().rdd.flatMap(lambda x: x).collect()

    # Here we filter the original dataframe to include only the top companies and join them with the annual returns
    df_top10_filtered = df.filter(col("Company").isin(top_companies_list))

    # Here we needed to name the DataFrames diferently for clarity, otherwise we were getting an Exception error
    df_alias = df.alias("original")
    df_annual_returns_alias = df_annual_returns.alias("annual_returns")
    df_top10_annual_returns = df_alias.join(df_annual_returns_alias, (col("original.Company") == col("annual_returns.Company")) & (col("original.Year") == col("annual_returns.Year")), "inner") \
        .select(col("original.Company"), col("original.Year"), col("annual_returns.AnnualReturn")) \
        .filter(col("original.Company").isin(top_companies_list)) \
        .distinct() \
        .orderBy("original.Company", "original.Year")

    # Download file
    market_name = path.split('/')[-2]
    output_path = f'/user/s2261294/ANUAL_RETURN/{market_name}'
    df_top10_annual_returns.write.csv(output_path, mode="ignore", header=True)
