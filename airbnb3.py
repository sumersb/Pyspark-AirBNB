from pyspark import SparkConf, SparkContext
import uuid
import csv
import sys

# spark-submit --conf "spark.pyspark.python=C:\Users\ahuja\anaconda3\python.exe" airbnb2.py C:/Users/ahuja/Downloads/airbnb/usa/Seattle/filtered_listings_detailed.csv C:/Users/ahuja/Downloads/airbnb/usa/Seattle/output accommodates 2 beds 2 50 100

def main():


    conf = SparkConf().setAppName("Airbnb3")
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
            if all(field_matches_filter(fields, filter_values[i], filter_values[i + 1]) for i in range(0, len(filter_values), 2)):
                # print("found a match")
                return True
        except Exception as e:
            pass
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
    aggfile_path = sys.argv[3]
    filter_values = sys.argv[4:]

    rdd = sc.textFile(input_path)
    filtered_rdd = rdd.filter(filter_records)
    mapped_rdd = filtered_rdd.map(emit_key_value)
    grouped_rdd = mapped_rdd.groupByKey()
    # After the grouped_rdd transformation
    top_10_rdd = grouped_rdd.flatMap(lambda x: x[1])  # Flatten the grouped RDD to get all records
    top_10_rdd = sc.parallelize(top_10_rdd.top(10, key=lambda x: float(next(csv.reader([x]))[16])))
    top_10_rdd.saveAsTextFile(output_path)  # Save top 10 records as a single file or set of files

    import os

    # Function to filter filenames based on pattern
    # def filter_filenames(directory):
    #     filenames = os.listdir(directory)
    #     csv_filenames = [filename for filename in filenames if filename.startswith("part") and filename.endswith(".csv")]
    #     return csv_filenames

    # # Get CSV filenames in the specified directory
    # csv_filenames = filter_filenames(aggfile_path)  # Assuming args[3] contains the directory path

    # if csv_filenames:
    #     # Assuming there's only one CSV file that matches the pattern, you can choose the first one
    #     file_path = os.path.join(aggfile_path, csv_filenames[0])
    #     neighbourhood_price_rdd = sc.textFile(file_path)
    # else:
    #     print("No CSV file found matching the pattern 'part-*.csv' in the specified directory.")


    neighbourhood_price_rdd = sc.textFile(aggfile_path)
    
    def parse_neighbourhood_price(line):
        try:
            cols = next(csv.reader([line]))  # Use csv.reader to handle CSV parsing
            if len(cols) >= 3:  # Ensure the line has at least 3 columns
                neighbourhood = cols[0]  # Assuming neighbourhood is at index 0
                neighbourhood_cleansed = cols[1]  # Assuming neighbourhood_cleansed is at index 1
                price = float(cols[2])  # Assuming price is at index 2
                return ((neighbourhood, neighbourhood_cleansed), price)
            else:
                return (None, None)  # Return None values for invalid lines
        except Exception as e:
            print("Error parsing line:", e)
            return (None, None)  # Return None values if parsing fails



# Create neighbourhood price map
    neighbourhood_price_map = neighbourhood_price_rdd \
        .map(parse_neighbourhood_price) \
        .collectAsMap()
    # neighbourhood_price_map = neighbourhood_price_rdd \
    #     .map(lambda line: line.split(",")) \
    #     .map(lambda cols: ((cols[0], cols[1]), float(cols[2]))) \
    #     .collectAsMap()
    
    def label_high_low(record):
        fields = next(csv.reader([record]))
        neighbourhood = fields[5]  # Assuming neighbourhood is at index 5
        neighbourhood_cleansed = fields[6]  # Assuming neighbourhood_cleansed is at index 6
        price = float(fields[13])  # Assuming price is at index 13
        
        # Try to get price for both neighbourhood and neighbourhood_cleansed
        neighbourhood_price = neighbourhood_price_map.get((neighbourhood, neighbourhood_cleansed))
        
        if neighbourhood_price:
            if price > neighbourhood_price:
                return "High"
            else:
                return "Low"
        else:
            return "Unknown"
    
    labelled_top_10 = [(label_high_low(record), record) for record in top_10_rdd.collect()]

    # Save labelled top 10 records to a file
    sc.parallelize(labelled_top_10).saveAsTextFile(output_path + "_labelled")

    sc.stop()


if __name__ == "__main__":
    main()
