import pymysql  # connect to rds--mysql
import time  # query time
import memcache  # importing memcache
import hashlib  # for hashing the queries
import csv
import os
from flask import Flask, request, make_response, render_template
import boto3
import botocore
from botocore.client import Config
from random import randint

ACCESS_KEY_ID = '*********************'
ACCESS_SECRET_KEY = '******************************************'

s3 = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY

)

# connecting to memcache
memc = memcache.Client(['*************.******.***.****.*****.*********.***:******'], debug=0)

# credentials to connect to the database
hostname = '************-***.************.**-****-*.***.*********.***'
username = '*****_*********'
password = '***********'
database = '******'
myConnection = pymysql.connect(host=hostname, user=username, passwd=password, db=database, charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor, local_infile=True)

print "connection successeded"

cur = myConnection.cursor()

app = Flask(__name__)


@app.route('/')
def hello_world():
    return render_template('webpageas4.html')


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['file']
    file_name = file.filename
    data = file.read()

    # upload
    s3.Bucket('Athmybuck').put_object(Key=file_name, Body=data)

    cur = myConnection.cursor()
    q1 = "create table dataquiz1 (Gender text,GivenName text,Surname text,StreetAddress text,City text,State text,EmailAddress text, Username text,TelephoneNumber text,Age text,BloodType text,Centimeters text,Latitude Double,Longitude Double)"
    cur.execute(q1)
    print "Table created"
    q2 = """LOAD DATA LOCAL INFILE 'D:/Masters/Cloud Computing/Cloud Assignments/Assignment4/data.csv' INTO TABLE dataquiz1 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' Lines terminated by '\n' IGNORE 1 LINES"""
    cur.execute(q2)
    myConnection.commit()
    cur = myConnection.cursor()
    q3 = "select Count(*) from dataquiz1"
    cur.execute(q3)
    result = cur.fetchall()  # contains all the results

    c = 0
    str1 = " "
    for row in result:
        c = c + 1

        str1 += str(c) + ':' + str(row) + '<br><br>'
    return  str1




@app.route('/limit', methods=['POST', 'GET'])
def query1():
    cur = myConnection.cursor()
    mquery = 'select FirstName,BirthYear from healthcare where BirthYear=1967 limit 1000'  # + str(rgen)
    mqhash = hashlib.sha256(mquery).hexdigest()
    starttime = time.time()
    #    for i in range(1, 2):
    #        rgen = randint(1965, 1970)

    cur.execute(mquery)
    endtime = time.time()
    tt = endtime - starttime
    result = cur.fetchall()
    memc.set(mqhash, result)

    starttime = time.time()

    res = memc.get(mqhash)
    print res
    endtime = time.time()
    tt1 = endtime - starttime

    return 'Time Taken by RDS:' + str(tt) + '<br><br>' + 'Time taken by Memcache:' + str(tt1) + '<br><br>' + str(res)


#    file_name='data1.csv'
#    cur=myConnection.cursor()

#    limit_number = request.form['limit']
#    starttime = time.time()


#    cur.execute('call random_Query(' + limit_number + ')')
#    result = cur.fetchall()  # contains all the results

#    endtime = time.time()
#    tt = endtime - starttime
#    str1 = 'RDS,'+str(limit_number)+',' + str(tt)+"\n"
#    with open(file_name,'a') as file1:
#        file1.write(str1)


#    return str(tt)









@app.route('/rds', methods=['POST', 'GET'])
def rds():
    num1=request.form['loop1']
    cur = myConnection.cursor()
    starttime = time.time()
    for i in range(1, int(num1)):
        rgen = randint(1965, 1970)
        mquery1 = 'select FirstName,BirthYear from healthcare where BirthYear=' + str(rgen)
        # mquery = 'select * from healthcare where longitude=-122.772499';
        cur.execute(mquery1)
    endtime = time.time()
    tt1 = endtime - starttime
    return str(tt1)


@app.route('/mem', methods=['POST', 'GET'])
def mem():
    num2 = request.form['loop2']
    cur = myConnection.cursor()

    starttime = time.time()
    for i in range(1, int(num2)):
        rgen = randint(1965, 1970)
        mquery = 'select FirstName,BirthYear from healthcare where BirthYear=' + str(rgen)
        # mquery = 'select * from healthcare where longitude=-122.772499';
        mqhash = hashlib.sha256(mquery).hexdigest()
        res = memc.get(mqhash)
        if not res:
            value=cur.execute(mquery)
            memc.set(mqhash, value)


    endtime = time.time()
    tt = endtime - starttime

    return  str(tt)

@app.route('/memdata', methods=['POST', 'GET'])
def memdata():
    cur = myConnection.cursor()
    result2=[]
    starttime = time.time()
    for i in range(1, 3):
        rgen = randint(1965, 1970)
        mquery = 'select FirstName,BirthYear from healthcare where BirthYear=' + str(rgen)
        # mquery = 'select * from healthcare where longitude=-122.772499';
        mqhash = hashlib.sha256(mquery).hexdigest()
        res = memc.get(mqhash)
        if not res:
            cur.execute(mquery)
            value=cur.fetchall()
            memc.set(mqhash, value)
            result2.append(value)
        else:
            result2.append(res)

    endtime = time.time()
    tt = endtime - starttime

    d = 0
    str2 = " "

    for row in result2:
        d = d + 1

        str2 += str(d) + ':' + str(row) + '<br><br><br>'
        #   print result


    return  str(tt) + '<br><br><br>' + str2

@app.route('/usrmemrds', methods=['POST', 'GET'])
def memdataquery():
    cur = myConnection.cursor()
    q1=request.form['q1']

    num3=request.form['loop3']
    if request.form['usr'] == "RDS":
        starttime = time.time()
        for i in range(1, int(num3)):
            rgen = randint(1965, 1970)
            mquery1 = q1 + str(rgen)

            cur.execute(mquery1)
        endtime = time.time()
        tt1 = endtime - starttime
        return str(tt1)

    elif request.form['usr'] == "MEMCACHE":
        starttime = time.time()
        for i in range(1, int(num3)):
            rgen = randint(1965, 1970)
            mquery = q1 + str(rgen)

            mqhash = hashlib.sha256(mquery).hexdigest()
            res = memc.get(mqhash)
            if not res:
                value = cur.execute(mquery)
                memc.set(mqhash, value)

        endtime = time.time()
        tt = endtime - starttime
        return str(tt)

@app.route('/quiz1', methods=['POST', 'GET'])
def quiz1():
    cur = myConnection.cursor()
    surname=request.form['surname']


    if request.form['quiz1'] == "RDS":
        starttime = time.time()


        mquery1 = 'select GivenName,TelephoneNumber,State from dataquiz1 where Surname=' +surname

        cur.execute(mquery1)
        result=cur.fetchall()
        endtime = time.time()
        tt1 = endtime - starttime
        str3 = " "
        c = 0
        for row in result:
            r = {}
            c = c + 1
            str3 += str(c) + ':' + str(row) + '<br><br>'
            print('row returned')
        myConnection.commit()



        return str(tt1)+'<br><br>'+str3

    elif request.form['quiz1'] == "MEMCACHE":
        starttime = time.time()

        mquery1 = 'select GivenName,TelephoneNumber,State from dataquiz1 where Surname=' + surname

        mqhash = hashlib.sha256(mquery1).hexdigest()
        res = memc.get(mqhash)
        if not res:
            value = cur.execute(mquery1)
            memc.set(mqhash, value)

        endtime = time.time()
        tt = endtime - starttime
        return str(tt)



@app.route('/quiz2', methods=['POST', 'GET'])
def quiz2():
    cur = myConnection.cursor()
    state=request.form['state']
    range1=request.form['range1']
    range2 = request.form['range2']
    num=request.form['loopquiz1']


    if request.form['quiz2'] == "RDS":
        starttime = time.time()




        mquery1 = 'select Age from dataquiz1 where Centimeters between ' +range1+ ' and ' +range2+ 'and  State=' +state

        cur.execute(mquery1)
        result=cur.fetchall()

        endtime = time.time()
        tt1 = endtime - starttime
        str3 = " "
        c = 0
        for row in result:
            r = {}
            c = c + 1
            str3 += str(c) + ':' + str(row) + '<br><br>'
            updatequery='update dataquiz1 set Age=' +str(row+1)+ 'where Age=' + str(row)+ 'where Gender="female" and Centimeters between ' +range1+ ' and ' +range2+ 'and  State=' +state
            cur.execute(updatequery)
            updatequery1 = 'update dataquiz1 set Age=' + str(row - 1) + 'where Age=' + str(row) + 'where Gender="male" and Centimeters between ' + range1 + ' and ' + range2 + 'and  State=' + state
            cur.execute(updatequery1)
            print('row returned')


        mquery2='select count(*) from dataquiz1 where Centimeters between ' +range1+ ' and ' +range2+ 'and  State=' +state
        cur.execute(mquery2)
        result1=cur.fetchall()
        myConnection.commit()

        str4 = " "
        c = 0
        for row in result1:
            r = {}
            c = c + 1
            str4 += str(c) + ':' + str(row) + '<br><br>'
            print('row returned')

        return str(tt1)+'<br><br>'+str4+'<br><br>'+str3

    elif request.form['quiz2'] == "MEMCACHE":
        starttime = time.time()

        for i in range(1, int(num)):
            rgen = randint(int(range1), int(range2))
            mquery1 = 'select GivenName,Centimeters,Age from dataquiz1 where Centimeters= ' +str(rgen)+ ' and State=' +state

            mqhash = hashlib.sha256(mquery1).hexdigest()
            res = memc.get(mqhash)
            if not res:
                value = cur.execute(mquery1)
                memc.set(mqhash, value)

        endtime = time.time()
        tt = endtime - starttime
        return str(tt)

if __name__ == "__main__":
    app.run(debug=True)
