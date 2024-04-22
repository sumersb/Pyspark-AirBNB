from pyspark import SparkConf, SparkContext
import uuid
import csv
import sys

# spark-submit --conf "spark.pyspark.python=C:\Users\ahuja\anaconda3\python.exe" airbnb2.py C:/Users/ahuja/Downloads/airbnb/usa/Seattle/filtered_listings_detailed.csv C:/Users/ahuja/Downloads/airbnb/usa/Seattle/output accommodates 2 beds 2 50 100

def main():


    conf = SparkConf().setAppName("Airbnb2")
    sc = SparkContext(conf=conf)


    def field_matches_filter(fields, column_name, value):
        try:
            column_index = column_map.get(column_name)
            if column_index is not None:
                field_value = fields[column_index]
                if "neighbourhood" in column_name.lower():
                    return str(value).lower() in str(field_value).lower()
                else:
                    return float(field_value) == float(value)
        except Exception as e:
            print("Exception occurred:", e)
        return False

        
    def filter_records(record):
        try:
            fields = next(csv.reader([record]))
            # Check if it's a header row
            if fields[0] == 'id' and fields[1] == 'name':  # Adjust header fields as per your CSV file
                return False  # Skip header row
            # Check filter conditions
            if all(field_matches_filter(fields, filter_values[i], filter_values[i + 1]) for i in range(0, len(filter_values) - 2, 2)) and price_is_in_range(float(fields[13])):
                # print("found a match")
                return True
        except Exception as e:
            pass
        return False

    def price_is_in_range(price):
        length = len(filter_values)
        try:
            return float(filter_values[length-2]) <= price and price <= float(filter_values[length-1])
        except:
            return False

    def emit_key_value(record):
        return (str(uuid.uuid4()), record)

    column_map = {
        "accommodates": 10,  # Assuming bedroom column is at index 10
        "bedroom": 11,  
        "bathroom": 17,
        "beds": 12,
        "neighbourhood": 5,
        "neighbourhood_cleansed": 6
        # Add more mappings as needed
    }

    # filter_values = ["accommodates", 2,"beds", 2,50,100]  # Add your filter values here

    # input_path = "C:/Users/ahuja/Downloads/airbnb/usa/Seattle/filtered_listings_detailed.csv"
    # output_path = "C:/Users/ahuja/Downloads/airbnb/usa/Seattle/output"spark-submit \
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    filter_values = sys.argv[3:]
    for i in filter_values:
        print(i)
    rdd = sc.textFile(input_path)
    filtered_rdd = rdd.filter(filter_records)
    mapped_rdd = filtered_rdd.map(emit_key_value)
    grouped_rdd = mapped_rdd.groupByKey()
    # After the grouped_rdd transformation
    top_10_rdd = grouped_rdd.flatMap(lambda x: x[1])  # Flatten the grouped RDD to get all records
    top_10_rdd = sc.parallelize(top_10_rdd.top(10, key=lambda x: float(next(csv.reader([x]))[16])))
    top_10_rdd.saveAsTextFile(output_path)  # Save top 10 records as a single file or set of files

    sc.stop()


if __name__ == "__main__":
    main()
