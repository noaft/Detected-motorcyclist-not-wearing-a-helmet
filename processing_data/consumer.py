from kafka import KafkaConsumer
import cv2
import numpy as np

# Thông số Kafka
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'video_test'

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap_servers': kafka_bootstrap_servers,
    'group_id': 'video_consumer_group',
    'auto_offset_reset': 'earliest'
}

# Create a Kafka Consumer
consumer = KafkaConsumer(kafka_topic, **consumer_conf)

# OpenCV VideoWriter
fourcc = cv2.VideoWriter_fourcc(*'XVID')
out = cv2.VideoWriter('output_video.avi', fourcc, 20.0, (640, 480))

# Read and process video frames from Kafka
for message in consumer:
    # Decode the frame from bytes
    img_bytes = np.frombuffer(message.value, dtype=np.uint8)
    frame = cv2.imdecode(img_bytes, cv2.IMREAD_COLOR)

    # Process the frame (you can add your processing logic here)

    # Display or save the frame
    cv2.imshow('Frame', frame)
    out.write(frame)

    # Break the loop if 'q' is pressed
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# Release the VideoWriter and close the Kafka Consumer
out.release()
cv2.destroyAllWindows()
consumer.close()
