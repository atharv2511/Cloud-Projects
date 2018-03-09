import os
from flask import Flask, request, make_response
from base64 import b64encode, b64decode
import time

from cloudant.client import Cloudant
from cloudant.document import Document

USERNAME="********-********-****-***********-bluemix"
PASSWORD="******************************************************"
client = Cloudant(USERNAME,PASSWORD, url='https://********-********-****-***********-bluemix:******************************************************-bluemix.cloudant.com')

client.connect()

ENCODING = 'utf-8'

#my_database=client.create_database('my_storage')
my_database = client['my_storage']

app = Flask(__name__)

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
print(APP_ROOT)
@app.route('/')
def index():
    return app.send_static_file('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    file_name = file.filename
    content = file.read()
    uploaded_file_content = b64encode(file.read())
    base64_string = uploaded_file_content.decode(ENCODING)
    last_modified_date = time.ctime(os.path.getmtime(file_name))
    # print (last_modified_date)
    for document in my_database:
        if (file_name == document['file_name'] and content.decode('utf-8') == document['content']):
            # print (document['file_name'])
            return "Warning!! Same file...Please go back"
    v = 0
    for document in my_database:
        if (file_name == document['file_name'] and not content.decode('utf-8') == document['content']):
            version = document['version number']
            if int(version) > v:
                v = int(version)
                # v=document['version number']
                # print (v)
                #	data = {'file_name': file_name, 'content': content.decode('utf-8'),'last modified date':last_modified_date,'version number':int(v)+1}
                #	doc = my_database.create_document(data)
                # else:
                #	data = {'file_name': file_name, 'content': content.decode('utf-8'),'last modified date':last_modified_date,'version number':'1'}
                #	doc = my_database.create_document(data)
    if v == 0:
        data = {'file_name': file_name, 'content': content.decode('utf-8'), 'last modified date': last_modified_date,
                'version number': '1'}
        doc = my_database.create_document(data)
    else:
        data = {'file_name': file_name, 'content': content.decode('utf-8'), 'last modified date': last_modified_date,
                'version number': int(v) + 1}
        doc = my_database.create_document(data)

    if doc.exists():
        return "Successfully Uploaded!!" + '<br><form action="../"><input type="Submit" value="Lets go back"></form>'


@app.route('/list', methods=['POST'])
def list():
    # list of files
    # print('hello!!')

    files = "<b>No Files are stored</b>"
    files1 = ""
    files2 = ""
    for document in my_database:
        files1 = files1 + '<li>File Name: {0}</br>\n Version no: {1}</br>\n Last Modified Date: {2}'.format(
            document['file_name'], document['version number'], document['last modified date']) + '</li></p>'
        files = ""
        files2 = '<h3>The files currently on my_storage are </h3><br><br><ol>'
    return files + files2 + files1 + '<br><form action="../"><input type="Submit" value="Lets go back"></form>'


@app.route('/download', methods=['POST'])
def download():
    file_name = request.form['filename']
    version_number = request.form['version number']
    # print (file_name)
    # print (version_number)
    for document in my_database:
        if (document['file_name'] == file_name and document['version number'] == version_number):
            # print(document['file_name'])
            downloaddoc = "<b>The requested document is downloaded!!</b>"
            with open(file_name, 'w') as outfile:
                outfile.write(document['content'])
            break
        # fileContents = k.decrypt(document).decode('UTF-8')
        # file_downloaded.write(fileContents)
        else:
            downloaddoc = "<b>File not found!!</b>"
    return downloaddoc + '<br><form action="../"><input type="Submit" value="Lets go back"></form>'


@app.route('/delete', methods=['POST'])
def delete():
    file_name = request.form['filename']
    version_number = request.form['version number']
    for document in my_database:
        if (document['file_name'] == file_name and document['version number'] == version_number):
            document.delete()
            df = "<b>The requested document is deleted</b>"
            break
        else:
            df = "<b>File not found!!</b>"
    return df + '<br><form action="../"><input type="Submit" value="Lets go back"></form>'


if __name__ == "__main__":
	app.run(debug=True)