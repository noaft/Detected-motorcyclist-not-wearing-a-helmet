from collections import defaultdict
import cv2
import numpy as np
from ultralytics import YOLO
import os

class_name = ['helmet', 'no-helmet']

# Load the YOLOv8 model
model = YOLO('D:/Python/best1.pt')

# Open the video file
video_path = 'D:/Python/@a.mp4'
cap = cv2.VideoCapture(video_path)

# Store the track history
track_history = defaultdict(lambda: [])

# Thư mục để lưu trữ hình ảnh của đối tượng "no-helmet"
output_dir = "D:/Python/no_helmet_images/"
os.makedirs(output_dir, exist_ok=True)
noloop = []
# Loop through the video frames
while cap.isOpened():
    # Read a frame from the video
    success, frame = cap.read()

    if success:
        # Run YOLOv8 tracking on the frame, persisting tracks between frames
        results = model.track(frame, persist=True)

        # Check if there are any detections in the frame
        if results and results[0].boxes.id is not None:
            # Get the boxes, labels, and track IDs
            for result in results:
                boxes = result.boxes.xyxy.cpu().numpy()
                track_ids = result.boxes.id.cpu().numpy()
                labels = result.boxes.cls.cpu().numpy()

                # Visualize the results on the frame
                annotated_frame = result.plot()

                # Loop through each detected object
                for box, track_id, label in zip(boxes, track_ids, labels):
                    x, y, w, h = box[:4]
                    label = int(label)
                    # Check if the object is "no-helmet"
                    if class_name[label] == 'no-helmet' and track_id not in noloop:
                        # Crop the object from the frame
                        cropped_object = frame[int(y):int(y + h), int(x):int(x + w)]
                        noloop.append(track_id)
                        # Save the cropped object image
                        image_name = f"no_helmet_{track_id}.jpg"
                        image_path = os.path.join(output_dir, image_name)
                        cv2.imwrite(image_path, cropped_object)

                # Display the annotated frame
                cv2.imshow("YOLOv8 Tracking", annotated_frame)

        # Break the loop if 'q' is pressed
        if cv2.waitKey(1) & 0xFF == ord("q"):
            break
    else:
        # Break the loop if the end of the video is reached
        break

# Release the video capture object and close the display window
cap.release()
cv2.destroyAllWindows()
