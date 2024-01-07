import cv2
import numpy as np
from pymongo import MongoClient
from io import BytesIO
import zlib
import base64

# Kết nối đến MongoDB
client = MongoClient("localhost", 27017)
db = client["Traffic"]
collection = db["images"]

# Process each row
for row in rows:
    # Get image data from the row
    img_data = row[0]

    # Decode the image data
    img_array = np.frombuffer(img_data, np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

    # Chuyển ảnh thành định dạng chuỗi và nén dữ liệu
    _, img_encoded = cv2.imencode(".jpg", img)
    img_bytes = BytesIO(zlib.compress(img_encoded.tobytes()))

    # Convert dữ liệu thành base64 để lưu vào MongoDB
    img_base64 = base64.b64encode(img_bytes.read()).decode("utf-8")

    # Lưu ảnh vào MongoDB
    document = {"image_data": img_base64}
    collection.insert_one(document)

# Đóng kết nối MongoDB
client.close()
