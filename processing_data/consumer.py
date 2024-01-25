import cv2
import numpy as np
from ultralytics import YOLO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import partial
from datetime import datetime
from pymongo import MongoClient
from PIL import Image
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image
from pymongo import MongoClient
from PIL import Image
import io
import matplotlib.pyplot as plt
import tensorflow as tf
# Global Constants
kafka_bootstrap_servers = 'localhost:9092' #local host kafka in local sever
topic_name = 'video_test' #topic in kafka
class_name = ['helmets', 'no-helmets'] # label
noloop = [] # Avoid duplicate photos
import io
model_path = 'D:/Python/cnn.h5'
cnn_model = load_model(model_path)
#func save_data_to_postgresql to save data and date to posgresql
def save_data_to_mongodb(frame, date):
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
        'track_id': track_id,
        'label_id': '0'
    }

    # Insert the document into the collection
    result = collection.insert_one(document)
    inserted_id = result.inserted_id

    return inserted_id

def save_data_to_mongodb_quantity(date):
    # Connect to MongoDB
    client = MongoClient("localhost", 27017)
    db = client["Traffic"]
    collection = db["images"]

    # Create a document to insert into MongoDB
    document = {
        'date': date,
        'label_id': '1'
    }

    # Insert the document into the collection
    result = collection.insert_one(document)
    inserted_id = result.inserted_id

    return inserted_id


model = YOLO('D:/Python/best.pt')
# process each line
def process_row(row):
    # Extract the decoded image
    frame = row['value']
    frame = np.frombuffer(frame, np.uint8)
    frame = cv2.imdecode(frame, cv2.IMREAD_COLOR)
    height, width, _ = frame.shape
    print(f"Width (x): {width}, Height (y): {height}")
    # Check if frame is not None before displaying
    if frame is not None:
        # YOLOv8 model initialization
        results_yolo = model.track(frame,persist=True)

        if results_yolo and results_yolo[0].boxes.id is not None:
            for result_yolo in results_yolo:
                # Take box, track_ids, labels, confs in frame from YOLO
                boxes_yolo = result_yolo.boxes.xyxy.cpu().numpy()
                track_ids_yolo = result_yolo.boxes.id.cpu().numpy()
                labels_yolo = result_yolo.boxes.cls.cpu().numpy()
                confs_yolo = result_yolo.boxes.conf.cpu().numpy()

                # Take each of box, track_ids, labels, confs in frame
                for box_yolo, track_id_yolo, label_yolo, conf_yolo in zip(boxes_yolo, track_ids_yolo, labels_yolo, confs_yolo):
                    x, y, w, h = box_yolo[:4]
                    label_yolo = int(label_yolo)
                    conf_yolo = float(conf_yolo)

                    if track_id_yolo not in noloop :
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        track_id_yolo = int(track_id_yolo)

                        noloop.append(track_id_yolo)
                        cropped_object_yolo = frame[int(y):int(h), int(x):int(w)]
                        cropped_object_resized = tf.image.resize(cropped_object_yolo, (224, 224))
                        # Apply the CNN model to the cropped object from YOLO
                        cnn_predictions = cnn_model.predict(np.expand_dims(cropped_object_resized / 255.0, axis=0))
                        predicted_class = '0' if cnn_predictions[0][1] > cnn_predictions[0][0] else '1'
                        if predicted_class == '0':
                            save_data_to_mongodb(cropped_object_yolo, timestamp)
                        else:
                            save_data_to_mongodb_quantity( timestamp)
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