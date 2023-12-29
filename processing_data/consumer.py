from kafka import KafkaConsumer
import cv2
import numpy as np
from ultralytics import YOLO
import os
import psycopg2
import psycopg2
# Kafka parameters

noloop = []

# Consume video frames from Kafka

def save_data_to_postgresql(frame, date, id):
    _, img_encoded = cv2.imencode('.jpg', frame)
    img_bytes = img_encoded.tobytes()

    # Connect to PostgreSQL
    conn = connect_postgresql()

    # Create a cursor
    cursor = conn.cursor()

    # SQL query with placeholders
    query = "INSERT INTO images (id, img_, date_) VALUES(%s, %s, %s)"

    # Execute the query with actual values
    cursor.execute(query, (id, img_bytes, date))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

def connect_postgresql():
    conn_params = {
        'database': 'datacamp_courses',
        'user': 'postgres',
        'password': '121203Toan',
        'host': 'localhost',
        'port': 5432
    }

    connection = psycopg2.connect(**conn_params)
    return connection


def combine_model(consumer, output_dir, model):
    for message in consumer:
    # Check if it's the end of the video
        if message.value is None:
            break
        # Convert the received bytes back to a frame
        nparr = np.frombuffer(message.value, np.uint8)
        print(nparr)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # YOLOv8 tracking
        results = model.track(frame, persist=True)

        if results and results[0].boxes.id is not None:
            for result in results:
                boxes = result.boxes.xyxy.cpu().numpy()
                track_ids = result.boxes.id.cpu().numpy()
                labels = result.boxes.cls.cpu().numpy()
                confs = result.boxes.conf.cpu().numpy()

                annotated_frame = result.plot()

                for box, track_id, label, conf in zip(boxes, track_ids, labels, confs):
                    x, y, w, h = box[:4]
                    label = int(label)
                    conf = float(conf)

                    if class_name[label] == 'no-helmet' and track_id not in noloop and conf > 0.4:
                        cropped_object = frame[int(y):int(y + h), int(x):int(x + w)]
                        noloop.append(track_id)
                        # save_data_to_postgresql(cropped_object, )
                        image_name = f"no_helmet_{track_id}.jpg"
                        image_path = os.path.join(output_dir, image_name)
                        cv2.imwrite(image_path, cropped_object)

                cv2.imshow("YOLOv8 Tracking", annotated_frame)

        if cv2.waitKey(1) & 0xFF == ord("q"):
            break

        cv2.destroyAllWindows()

if __name__ == '__main__':
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'video_test'

    # Kafka consumer configuration
    consumer_conf = {
        'bootstrap_servers': kafka_bootstrap_servers,
        'group_id': 'video_consumer',
        'auto_offset_reset': 'earliest'
    }

    # Create Kafka consumer
    consumer = KafkaConsumer(kafka_topic, **consumer_conf)

    # YOLOv8 model initialization
    class_name = ['helmet', 'no-helmet']
    model = YOLO('D:/Python/best1.pt')

    # Directory for saving 'no-helmet' images
    output_dir = "D:/Python/no_helmet_images/"
    os.makedirs(output_dir, exist_ok=True)
    combine_model(consumer, output_dir, model )