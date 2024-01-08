from pymongo import MongoClient
from PIL import Image
import io
import matplotlib.pyplot as plt

# Connect to MongoDB
client = MongoClient("localhost", 27017)
db = client["Traffic"]
collection = db["images"]

# Assuming you have a specific document ID, adjust accordingly

# Retrieve the document from MongoDB
document = collection.find_one()

# Check if the document exists
if document:
    # Extract the image data from the document
    image_data = document.get('image')

    # Convert the image data to a PIL Image
    pil_img = Image.open(io.BytesIO(image_data))

    # Display the image using Matplotlib
    plt.imshow(pil_img)
    plt.show()
else:
    print("Document not found.")
