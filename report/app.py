from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import DateField
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
import base64

app = Flask(__name__)
Bootstrap(app)

app.config['SECRET_KEY'] = 'your_secret_key'

# Connect to MongoDB
client = MongoClient("127.0.0.1", 27017)
db = client["report"]
collection = db["image"]

class DateForm(FlaskForm):
    date = DateField('Pick a Date', format='%Y-%m-%d')

def get_images_by_label_and_date(date, label):
    date_obj = pd.to_datetime(date)
    documents = collection.find({
        "date": {
            "$gte": date_obj,
            "$lt": date_obj + pd.DateOffset(days=1),
        },
        "label_id": label
    })
    return pd.DataFrame(list(documents))

def get_distribution_by_date(date):
    date_obj = pd.to_datetime(date)
    documents = collection.find({
        "date": {
            "$gte": date_obj,
            "$lt": date_obj + pd.DateOffset(days=1),
        }
    })

    # Ensure 'label_id' field is present in the result
    df = pd.DataFrame(list(documents), columns=['image', 'date', 'label_id'])
    
    print(df)  # Add this line to print the DataFrame
    return df

    date_obj = pd.to_datetime(date)
    documents = collection.find({
        "date": {
            "$gte": date_obj,
            "$lt": date_obj + pd.DateOffset(days=1),
        }
    })

    # Ensure 'label_id' field is present in the result
    df = pd.DataFrame(list(documents), columns=['image', 'date', 'label_id'])

    return df


def get_image_data_list(df):
    image_data_list = []
    for _, document in df.iterrows():
        image_data = document.get('image')
        label = document.get('label_id')

        im = BytesIO(image_data)
        im.seek(0)
        img_data = base64.b64encode(im.read()).decode('utf-8')

        image_data_list.append({'img_data': img_data, 'label': label})

    return image_data_list

@app.route('/', methods=['GET', 'POST'])
def index():
    form = DateForm()
    if form.validate_on_submit():
        date = form.date.data.strftime('%Y-%m-%d')
        return redirect(url_for('distribution', date=date))
    return render_template('index.html', form=form)

@app.route('/distribution/<date>')
def distribution(date):
    df = get_distribution_by_date(date)
    image_data_list = get_image_data_list(df)

    sizes = df['label_id'].value_counts().tolist()
    plt.figure()
    plt.pie(sizes, labels=['0', '1'], autopct='%1.1f%%', startangle=90)
    plt.title(f'Distribution for Date: {date}')
    img_path = 'static/images/pie_chart.png'
    plt.savefig(img_path)
    plt.close()

    return render_template('distribution.html', date=date, img_data_list=image_data_list, img_path=img_path)

@app.route('/show_images/<date>')
def show_images(date):
    df = get_images_by_label_and_date(date, label='0')
    image_data_list = get_image_data_list(df)
    return render_template('show_images.html', date=date, img_data_list=image_data_list)

if __name__ == '__main__':
    app.run(debug=True)
