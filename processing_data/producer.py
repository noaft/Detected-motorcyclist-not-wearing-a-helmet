from kafka import KafkaProducer
import cv2
import numpy
# Thông số Kafka

def producer_kafka(kafka_topic, video_path):
    cap = cv2.VideoCapture(video_path)

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
            # Send a flag to indicate the end of the video
            break

    # Release the video capture object outside the loop
    cap.release()

    # Flush the producer to make sure all messages are delivered
    producer.flush()

if __name__ == "__main__":
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'video_test'

    # Kafka Producer configuration
    producer_conf = {
        'bootstrap_servers': kafka_bootstrap_servers,
        'client_id': 'video_producer'
    }

    # Create a Kafka Producer
    producer = KafkaProducer(**producer_conf)

    # Open the video file
    video_path = 'D:/Python/Detected-motorcyclist-not-wearing-a-helmet/data/video_test.mp4'
    producer_kafka(kafka_topic, video_path)
