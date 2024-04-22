from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
import sys
def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Average Daily Price Calculation") \
        .getOrCreate()

    # Local
    input_path = sys.argv[1]

    df = spark.read.csv(input_path, header=True, multiLine=True, quote="\"",escape="\"")

    grouped_avg = df.groupBy("neighbourhood","neighbourhood_cleansed").agg(avg("price")).alias("avg_price")

    grouped_avg.show()

    output_path = sys.argv[2]

    # Compute average price
    grouped_avg.write.mode("overwrite").csv(output_path)

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()