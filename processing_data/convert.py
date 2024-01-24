from pymongo import MongoClient
from PIL import Image
import io
import matplotlib.pyplot as plt

# Connect to MongoDB
client = MongoClient("127.0.0.1", 27017)
db = client["Traffic"]
collection = db["images"]

# Retrieve the documents from MongoDB
documents = collection.find()

# Iterate through the documents

for document in documents:
    # Extract the image data from the document
    image_data = document.get('image')
    label = document.get('label_id')
    print(label)
    # Convert the image data to a PIL Image
    if label == '1':
        pil_img = Image.open(io.BytesIO(image_data))

        # Display the image using Matplotlib
        plt.imshow(pil_img)
        plt.show()
    