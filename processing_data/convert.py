import cv2
import numpy as np
import psycopg2
from io import BytesIO

# Database connection parameters
import pymongo
from pymongo import MongoClient
client = MongoClient("localhost", 27017)

# Process each row
for row in rows:
    # Get image data from the row
    img_data = row[0]

    # Decode the image data
    img_array = np.frombuffer(img_data, np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

    # Display or save the image as needed
    cv2.imshow("Retrieved Image", img)

    # Wait for a key event and check if it's the 'x' key
    key = cv2.waitKey(0)
    if key == ord('x'):
        # Close the current image window
        cv2.destroyAllWindows()
    else:
        # Continue to the next image in the loop
        continue
