from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
import cv2
import numpy as np


spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j-error.properties") \
    .getOrCreate()


# Define the Kafka topic and bootstrap servers
kafka_bootstrap_servers = 'localhost:9092'
topic_name = 'video_test'

# Đọc dữ liệu từ Kafka
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Define a UDF to decode the image data
value_column = kafka_df.select('value').first()['value']

# Register the UDF
nparr = np.frombuffer(value_column, np.uint8)
frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
i = 1
while i < 1000:
    # Đọc một frame mới từ video hoặc Kafka

    # Kiểm tra nếu không có frame nào được đọc
    if frame is None:
        break

    # Hiển thị frame
    cv2.imshow('Decoded Image', frame)

    # Đợi một khoảng thời gian ngắn (ví dụ: 25 ms)
    key = cv2.waitKey(25)

    # Nếu người dùng nhấn phím ESC, thoát khỏi vòng lặp
    if key == 27:
        break

    i += 1

# Giải phóng tài nguyên
cv2.destroyAllWindows()
cap.release()