from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# Connect to MongoDB
client = MongoClient("127.0.0.1", 27017)
db = client["Traffic"]
collection = db["images"]

# Query all documents from the collection
documents = collection.find()

# Create an empty DataFrame
data = {'date': [], 'label_0_count': [], 'other_count': []}
df = pd.DataFrame(data)

# Count the total number of images for each date
total_count_by_date = {}

# Iterate through the documents and count occurrences of label '0' and other labels for each date
for document in documents:
    date_time_str = document.get('date')
    date_time_obj = pd.to_datetime(date_time_str)  # Convert to datetime object
    date = date_time_obj.date()  # Extract only the date part
    label = document.get('label_id')
    
    # Update the total count for each date
    if date not in total_count_by_date:
        total_count_by_date[date] = 1
    else:
        total_count_by_date[date] += 1
    
    # Check if the label is '0'
    if label == '0':
        # If the date is not in the DataFrame, add it with count 1
        if date not in df['date'].tolist():
            df = df.append({'date': date, 'label_0_count': 1, 'other_count': 0}, ignore_index=True)
        else:
            # If the date is already in the DataFrame, increment the label '0' count
            df.loc[df['date'] == date, 'label_0_count'] += 1
    else:
        # If the date is not in the DataFrame, add it with count 1
        if date not in df['date'].tolist():
            df = df.append({'date': date, 'label_0_count': 0, 'other_count': 1}, ignore_index=True)
        else:
            # If the date is already in the DataFrame, increment the other label count
            df.loc[df['date'] == date, 'other_count'] += 1

# Plot the data as a pie chart for each date
for _, row in df.iterrows():
    labels = ['no-helmets', 'helmets']
    sizes = [row['label_0_count'], row['other_count']]
    plt.figure()
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    plt.title(f'Distribution for Date: {row["date"]}')
    plt.show()
