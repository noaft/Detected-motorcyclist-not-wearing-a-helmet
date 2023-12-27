import cv2
from kafka import KafkaProducer

# Thông số Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'video_topic'

# Open the video file
video_path = 'D:/Python/@a.mp4'
cap = cv2.VideoCapture(video_path)

# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': kafka_bootstrap_servers,
    'client.id': 'video_producer'
}

# Create a Kafka Producer
producer = KafkaProducer(**producer_conf)

# Read and send video frames to Kafka
while cap.isOpened():
    # Read a frame from the video
    success, frame = cap.read()

    if success:
        # Convert the frame to bytes (you may need to serialize it based on your use case)
        _, img_encoded = cv2.imencode('.jpg', frame)
        img_bytes = img_encoded.tobytes()

        # Send the frame to Kafka
        producer.send(kafka_topic, value=img_bytes)

        # Uncomment the following line to introduce a delay (in milliseconds) between frames
        # cv2.waitKey(25)

    else:
        break

# Release the video capture object
cap.release()

# Flush the producer to make sure all messages are delivered
producer.flush()
