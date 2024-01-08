import cv2
import numpy as np
from ultralytics import YOLO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import partial
from datetime import datetime
from pymongo import MongoClient
from PIL import Image
# Global Constants
kafka_bootstrap_servers = 'localhost:9092' #local host kafka in local sever
topic_name = 'video_test' #topic in kafka
class_name = ['helmets', 'no-helmets'] # label
noloop = [] # Avoid duplicate photos
import io

#func save_data_to_postgresql to save data and date to posgresql
def save_data_to_mongodb(frame, date, track_id):
    # Connect to MongoDB
    client = MongoClient("localhost", 27017)
    db = client["Traffic"]
    collection = db["images"]

    # Convert the NumPy array to a PIL Image
    im = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))

    # Convert the image to bytes
    image_bytes = io.BytesIO()
    im.save(image_bytes, format='JPEG')
    image_data = image_bytes.getvalue()

    # Create a document to insert into MongoDB
    document = {
        'image': image_data,
        'date': date,
        'track_id': track_id
    }

    # Insert the document into the collection
    result = collection.insert_one(document)
    inserted_id = result.inserted_id

    return inserted_id




model = YOLO('D:/Python/best1.pt')
# process each line
def process_row(row):
    # Extract the decoded image
    frame = row['value']
    frame = np.frombuffer(frame, np.uint8)
    frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
    # Check if frame is not None before displaying
    if frame is not None:
        # YOLOv8 model initialization

        # Display the frame
        results = model.track(frame, persist=True)

        if results and results[0].boxes.id is not None:
            for result in results :
                    #take box, track_ids, labels, confs in frame
                boxes = result.boxes.xyxy.cpu().numpy()
                track_ids = result.boxes.id.cpu().numpy()
                labels = result.boxes.cls.cpu().numpy()
                confs = result.boxes.conf.cpu().numpy()

                annotated_frame = result.plot()
                #take each of box, track_ids, labels, confs in frame
                for box, track_id, label, conf in zip(boxes, track_ids, labels, confs):
                    x, y, w, h = box[:4]
                    label = int(label)
                    conf = float(conf)

                    if class_name[label] == 'no-helmets' and conf > 0.4 and track_id not in noloop:
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        track_id = int(track_id)
                        noloop.append(track_id)
                        cropped_object = frame[int(y):int(y + h), int(x):int(x + w)]
                        save_data_to_mongodb(cropped_object, timestamp, track_id)



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

    partial_process_row = partial(process_row)
    query = kafka_df.writeStream \
        .foreach(partial_process_row) \
        .start()

    # Chờ truy vấn streaming kết thúc
    query.awaitTermination()

if __name__ == '__main__':
    take_data() # Wait for the thread to finish before exiting the main program