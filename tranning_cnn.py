import os
import numpy as np
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.applications import VGG16
from tensorflow.keras import layers
from tensorflow.keras import models
from tensorflow.keras import optimizers

# Đường dẫn đến thư mục chứa dữ liệu
data_dir = '/content/drive/MyDrive/img'

# Kích thước ảnh đầu vào sau khi resize
target_size = (224, 224)
batch_size = 32

# Sử dụng ImageDataGenerator để load và augment dữ liệu
train_datagen = ImageDataGenerator(
    rescale=1./255,
    shear_range=0.2,
    zoom_range=0.2,
    horizontal_flip=True,
    validation_split=0.2
)

train_generator = train_datagen.flow_from_directory(
    data_dir,
    target_size=target_size,
    batch_size=batch_size,
    class_mode='categorical',
    subset='training'
)

validation_generator = train_datagen.flow_from_directory(
    data_dir,
    target_size=target_size,
    batch_size=batch_size,
    class_mode='categorical',
    subset='validation'
)

# Load pre-trained model VGG16 without top layer
base_model = VGG16(weights='imagenet', include_top=False, input_shape=(target_size[0], target_size[1], 3))

# Freeze the convolutional layers
for layer in base_model.layers:
    layer.trainable = False

# Create a custom model
model = models.Sequential()
model.add(base_model)
model.add(layers.Flatten())
model.add(layers.Dense(256, activation='relu'))
model.add(layers.Dropout(0.5))
model.add(layers.Dense(2, activation='softmax'))

# Compile the model
model.compile(
    loss='categorical_crossentropy',
    optimizer=optimizers.Adam(learning_rate=1e-4),
    metrics=['accuracy']
)

# Train the model
history = model.fit(
    train_generator,
    steps_per_epoch=train_generator.samples // batch_size,
    epochs=10,
    validation_data=validation_generator,
    validation_steps=validation_generator.samples // batch_size
)

# Save the model
model.save('/content/drive/MyDrive/img/helmet_model.h5')
import os
import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing import image

# Đường dẫn đến mô hình đã lưu
model_path = '/content/drive/MyDrive/img/helmet_model.h5'
model = load_model(model_path)

# Đường dẫn đến ảnh mới cần dự đoán
image_path = '/content/Screenshot 2024-01-22 171916.png'  # Thay đổi đường dẫn tới ảnh mới

# Load ảnh và chuyển đổi kích thước
img = image.load_img(image_path, target_size=(224, 224))

# Chuyển ảnh thành mảng numpy
img_array = image.img_to_array(img)

# Mở rộng chiều của mảng (thêm chiều batch)
img_array = np.expand_dims(img_array, axis=0)

# Chuẩn hóa dữ liệu
img_array /= 255.0

# Dự đoán
predictions = model.predict(img_array)

# Lấy tên lớp dự đoán
if predictions[0][0] > predictions[0][1]:
    predicted_class = 'helmet'
else:
    predicted_class = 'no_helmet'

print(f"Predicted Class: {predicted_class}")
