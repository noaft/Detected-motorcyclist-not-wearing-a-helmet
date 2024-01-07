from pymongo import MongoClient
import base64
import zlib
import numpy as np
import cv2

# Connect to MongoDB
client = MongoClient("localhost", 27017)
db = client["Traffic"]
collection = db["images"]

# Use the aggregate method to match documents with the 'image' field
pipeline = [
    {
        "$match": {"image": {"$exists": True}}
    }
]

result_cursor = collection.aggregate(pipeline)

# Loop through the results
for document in result_cursor:
    # Retrieve the 'image_data' field
    image_data = document['image']
    print(image_data)
    try:
        # Decode base64 and decompress image data
        img_bytes = base64.b64decode(image_data)
        img_data = zlib.decompress(img_bytes)

        # Decode the image using OpenCV
        img_array = np.frombuffer(img_data, np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

        # Display the image
        cv2.imshow("Image", img)
        cv2.waitKey(1)  # Change waitKey to 1 for non-blocking display

    except zlib.error as e:
        print(f"Error decompressing data for document: {e}")

# Close MongoDB connection
client.close()

# Wait for a key press before closing the OpenCV window
cv2.waitKey(0)
cv2.destroyAllWindows()
