from kafka import KafkaConsumer
import cv2
import numpy as np
from ultralytics import YOLO
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BinaryType
from functools import partial
import threading

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

# Corrected file path and date format
file_path = r'D:/Python/BikesHelmets764.png'
date = '2001-11-30'  # Assuming the date format is YYYY-MM-DD
track_id = 1

# Read the image
frame = cv2.imread(file_path)

# Example usage
save_data_to_postgresql(frame, date, track_id)
