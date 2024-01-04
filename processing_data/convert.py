import cv2
import numpy as np
from ultralytics import YOLO
import psycopg2
from datetime import datetime

# Global Constants
kafka_bootstrap_servers = 'localhost:9092'
topic_name = 'video_test'
class_name = ['helmet', 'no-helmets']
noloop = []

# Đường dẫn đến video
video_path = 'D:/Python/Detected-motorcyclist-not-wearing-a-helmet/data/video_test.mp4'

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
    print("save")

    # Execute the query with actual values
    cur.execute(query, (track_id, img_bytes, date))

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

cap = cv2.VideoCapture(video_path)
model = YOLO('D:/Python/best1.pt')

while cap.isOpened():
    ret, frame = cap.read()

    if not ret:
        break

    # YOLOv8 prediction
    results = model.track(frame, persist=True)

    if results and results[0].boxes.id is not None:
        for result in results:
            boxes = result.boxes.xyxy.cpu().numpy()
            track_ids = (result.boxes.id.cpu().numpy()) 
            labels = result.boxes.cls.cpu().numpy()
            confs = result.boxes.conf.cpu().numpy()

            annotated_frame = result.plot()

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

        # Hiển thị hình ảnh với OpenCV
        cv2.imshow("Detected Objects", annotated_frame)

        # Đợi ấn phím bất kỳ từ người dùng để tiếp tục
        key = cv2.waitKey(1)

        # Nếu phím Esc (27) được ấn, thoát vòng lặp
        if key == 27:
            break

# Đóng cửa sổ hiển thị
cv2.destroyAllWindows()
