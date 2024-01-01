from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
import cv2
import numpy as np

# Create a Spark session
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j-error.properties") \
    .getOrCreate()

# Define the Kafka topic and bootstrap servers
kafka_bootstrap_servers = 'localhost:9092'
topic_name = 'video_test'

# Read data from Kafka
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Define a UDF to decode the image data
@udf(BinaryType())
def decode_image(value):
    nparr = np.frombuffer(value, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    return frame

# Apply the UDF to the DataFrame
kafka_df = kafka_df.withColumn('decoded_image', decode_image('value'))

# Define a processing function to display frames
def process_row(row):
    # Extract the decoded image
    frame = row['decoded_image']

    # Check if frame is not None before displaying
    if frame is not None:
        # Display the frame
        cv2.imshow('Decoded Image', frame)

        # Wait for a short duration (e.g., 25 ms)
        key = cv2.waitKey(25)

        # If the user presses ESC key, exit the loop
        if key == 27:
            exit()

# Apply the processing function to each row using foreach
kafka_df.foreach(process_row)

# Release resources
cv2.destroyAllWindows()
