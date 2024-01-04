from kafka import KafkaConsumer
import cv2
import numpy as np
from ultralytics import YOLO
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import partial
from datetime import datetime
# Global Constants
kafka_bootstrap_servers = 'localhost:9092' #local host kafka in local sever
topic_name = 'video_test' #topic in kafka
class_name = ['helmets', 'no-helmets'] # label
noloop = [] # Avoid duplicate photos


#func save_data_to_postgresql to save data and date to posgresql
def save_data_to_postgresql(frame, date, track_id):
    # Database connection parameters
    conn_params = {
        'database': 'customer',
        'user': 'postgres',
        'password': '121203Toan',
        'host': 'localhost',
        'port': 5432
    }

    # Connect to PostgreSQL
    conn = psycopg2.connect(**conn_params)

    # Encode the image
    _, img_encoded = cv2.imencode('.jpg', frame)
    img_bytes = img_encoded.tobytes()

    # Create a cursor
    cur = conn.cursor()

    # SQL query with placeholders
    query = "INSERT INTO images (track_id, img_, date_) VALUES (%s, %s, %s)"

    # Execute the query with actual values
    cur.execute(query, (track_id, img_bytes, date))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


# process each line
def process_row(row):
    # Extract the decoded image
    frame = row['decoded_image']
    frame = np.frombuffer(frame, np.uint8)
    frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
    # Check if frame is not None before displaying
    if frame is not None:
        # YOLOv8 model initialization
        model = YOLO('D:/Python/best1.pt')

        # Display the frame
        results = model.track(frame, persist=True)

        if results and results[0].boxes.id is not None:
            for result in results:
                #take box, track_ids, labels, confs in frame
                boxes = result.boxes.xyxy.cpu().numpy()
                track_ids = (result.boxes.id.cpu().numpy()) 
                labels = result.boxes.cls.cpu().numpy()
                confs = result.boxes.conf.cpu().numpy()

                annotated_frame = result.plot()
                #take each of box, track_ids, labels, confs in frame
                for box, track_id, label, conf in zip(boxes, track_ids, labels, confs):
                    x, y, w, h = box[:4]
                    label = int(label)
                    conf = float(conf)

                    if class_name[label] == 'no-helmets' and track_id not in noloop and conf > 0.4:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        track_id = int(track_id)
                        noloop.append(track_id)
                        cropped_object = frame[int(y):int(y + h), int(x):int(x + w)]
                        save_data_to_postgresql(cropped_object, timestamp, track_id)

            # display images
                cv2.imshow("Detected Objects", annotated_frame)

            # wait input from key by users
                key = cv2.waitKey(1)

            # if press Esc (27), exit the loop
                if key == 27:
                    break


def take_data():
    spark = SparkSession.builder \
        .appName("YourAppName") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j-error.properties") \
        .getOrCreate()

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()
    
    kafka_df = kafka_df.select("value", "timestamp")
    kafka_df = kafka_df.withColumn('decoded_image', col('value'))

    partial_process_row = partial(process_row)
    query = kafka_df.writeStream \
        .foreach(partial_process_row) \
        .start()

    # Chờ truy vấn streaming kết thúc
    query.awaitTermination()

if __name__ == '__main__':
    take_data() # Wait for the thread to finish before exiting the main program