import os
from flask import Flask, request, make_response, render_template
import boto3
import botocore
from botocore.client import Config
import time

ACCESS_KEY_ID = '*********************'
ACCESS_SECRET_KEY = '******************************************'

s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY

)

# s3 = boto3.client('s3')
# s3.create_bucket(Bucket='Athmybuck')
"""
data = open('Fire(2).jpg', 'rb')
    filename = 'Fire(2).jpg'
    bucket_name = 'Athmybuck'
    Key = 'Fire(2).jpg'
"""

app = Flask(__name__)


# APP_ROOT = os.path.dirname(os.path.abspath(__file__))
# print(APP_ROOT)


@app.route('/')
def index():
    #	return "Hello"
    return render_template('login.html')


@app.route('/login', methods=['POST', 'GET'])
def login():
    key = 'login.txt'
    bucket = s3.Bucket('Athmybuck')
    for obj in bucket.objects.all():
        if key == obj.key:
            body = obj.get()['Body'].read()

            username = request.form['uname']
            password = request.form['psw']

            username, password = body.split(':')

            if request.form['uname'] == username and request.form['psw'] == password:

                return render_template('index.html')
            else:
                return render_template('login.html')


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    file_name = file.filename
    file_number = request.form['file number']
    data = file.read()
    dict = {file_name: file_number}
    # upload
    s3.Bucket('Athmybuck').put_object(Key=file_name, Body=data, Metadata=dict)
    return "The file is uploaded !"


@app.route('/download', methods=['POST'])
def download():
    file_name = request.form['filename']
    Key = file_name
    href = 'https://s3.amazonaws.com/Athmybuck/%s' % Key
    return '<a href=%s>Downloaded</a>' % href


"""
  try:
        s3.Bucket('Athmybuck').download_file(Key, 'D:/%s'%file_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:

            raise
    return "The file is downloaded !"
"""


@app.route('/list', methods=['POST'])
def list():
    obj = ''
    for bucket in s3.buckets.all():
        for object in bucket.objects.all():
            obj = obj + '</br>' + object.key + '&nbsp' + str(object.size) + '&nbsp' + str(object.last_modified)
    return obj


@app.route('/delete', methods=['POST'])
def delete():
    file_name = request.form['filename']
    for bucket in s3.buckets.all():
        for object in bucket.objects.all():
            if object.key == file_name:
                object.delete()
                return "File has been deleted successfully"


@app.route('/display', methods=['POST'])
def display():
    file_name = request.form['filename']
    for bucket in s3.buckets.all():
        for object in bucket.objects.all():
            if file_name == object.key:
                file, ext = file_name.split('.')
                if ext == 'jpg':
                    return '<img src=https://s3.amazonaws.com/Athmybuck/%s>' % file_name
                elif ext == 'txt':
                    body = object.get()['Body'].read()
                    return '<pre>%s</pre>' % body


"""


try:
    s3.Bucket('Athmybuck').download_file(Key, 'file.jpg')
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "404":
        print("The object does not exist.")
    else:

        raise

for bucket in s3.buckets.all():
    for object in bucket.objects.all():
        print(object.key)
"""

if __name__ == "__main__":
    app.run(debug=True)
