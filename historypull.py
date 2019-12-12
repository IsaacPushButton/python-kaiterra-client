import requests, json
from datetime import datetime as dt
from datetime import timedelta
import json
import csv
import pymysql
import math
#def pull_history():
csv_columns = ['km100.rpm10c', 'km100.rpm25c', 'km102.rhumid', 'km102.rtemp', 'km102.rtvoc (ppb)', 'rco2 (ppm)', 'ts', 'Location', 'Device']
csv_file = "entries.csv"
dateformat = '%Y-%m-%dT%H:%M:%SZ'

def write_headers():
    try:
        with open(csv_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
    except IOError:
        print("I/O error")        

    
def readHistory(*devs, start =  None, finish = None):
    key = 'MGIwMTQ5MmIzY2IzNDkwYWI1YjViZmUwMGE3MThhNjM3ZTA3'
    
    #url = 'https://api.kaiterra.cn/v1/sensedges/{}'.format(dev)
    r = requests.post(
        'https://api.kaiterra.cn/v1/batch?include_headers=false',
        params= {'key': key},
        headers = {'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
        data = json.dumps(
            [{'method': 'GET', 'relative_url': 'sensedges/{}/history?series=raw&begin={}&end={}'.format(dev, start, finish)} for dev in devs]
            )
        )
    
    for response in r.json(): #json.loads(r.content)
        #print(response)
        yield json.loads(response['body'])
def connect_writer():
    host="testdev2.cluster-cqtvhklvqwhc.us-east-2.rds.amazonaws.com"
    port=3306
    dbname= "device"
    user="admin"
    password="SmellingSaltInfinite"

    conn = pymysql.connect(host, user=user,port=port,
                               passwd=password, db=dbname)
    return conn

def get_last_reading(deviceID):
    return last_reading
def load_config():
    with open('config.json') as f:
        json_data = json.load(f)
    return json_data


#def write_chunk


def create_insert_string(p):
    sql = "INSERT INTO `readings` (`pm10c`, `pm25c`, `humid`, `temp`, `tvoc`, `co2`, `ts`, `location`, `device`) VALUES ({}, {},{},{},{},{},'{}','{}','{}')"\
                .format(p['km100.rpm10c'], p['km100.rpm25c'], p['km102.rhumid'], p['km102.rtemp'], p['km102.rtvoc (ppb)'], p['rco2 (ppm)'], dt.strptime(p['ts'],dateformat).isoformat(), p['Location'], p['Device'])
    return sql
    
def write_chunk_to_db(chunk, sqlconnection):  
    with sqlconnection.cursor() as cursor:
        # Create a new record
        #sql = "USE device"
        #cursor.execute(sql)
        i = 0
        for row in chunk:
            try:
                print("Writing to database {}/{}. Timestamp: {}".format(i,len(chunk), row['ts']), flush = True)
                sql = create_insert_string(row)
                #print("Sending SQL: {}".format(sql))
                cursor.execute(sql)
                i += 1
            except pymysql.ProgrammingError as e :
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            except pymysql.DataError as e :
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            except pymysql.IntegrityError as e :
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            except pymysql.NotSupportedError as e :
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            except pymysql.OperationalError as e :
                print('Got error {!r}, errno is {}'.format(e, e.args[0]))
            except BaseException as e:
                print('Unknown error: {} '.format(str(e)))
    sqlconnection.commit()
        # connection is not autocommit by default. So you must commit to save
        # your changes.
    

    
def get_device_data_chunk(dev,location, sqlconnection, pulldate, chunkend):
    rindex = 0
    chunkdata = []
    for responses in readHistory(dev, start = pulldate.strftime(dateformat), finish= chunkend.isoformat().split('.')[0]+'Z'):
                print("reading response {}".format(str(rindex + 1)), flush=True)
                responseid = None
                for k in responses:
                    if k == 'id':
                        responseid = responses[k]
                    if k == 'data':
                        for p in responses[k]:
                            datachunk = p
                            datachunk['Device'] = responseid
                            datachunk['Location'] = location['ID']
                            chunkdata.append(p)
                            rindex += 1
    write_chunk_to_db(chunkdata,sqlconnection)

                                
    
def get_location_data(location):
    location_name = location['ID']
    location_devices = [location['Config']['Device UUIDs'][i] for i in location['Config']['Device UUIDs']]

    startDate =  '2019-12-10T00:00:00Z'
    endDate =    '2019-12-10T00:15:00Z'
    #startDate = get_last_reading()
    #endDate = dt.now()
    #devs = ['740c82ab-e4bb-4cea-b018-4dc0c5db0747']
    #devs = ['29fa3f23-2dfe-493a-b772-9e481622718d']
    tdelta = dt.strptime(endDate,dateformat) - dt.strptime(startDate,dateformat)
    chunksize = 3
    chunks = int(math.ceil(tdelta.days / chunksize))
    if chunks == 0: chunks = 1
    print("Breaking request into {} chunks".format(chunks), flush=True)
    sqlconnection = connect_writer()
    for i in range(chunks):
        print("Pulling chunk {}".format(str(i)), flush=True)
        readings = []
        pulldate = dt.strptime(startDate,dateformat) + (timedelta(days=chunksize) * i)
        chunkend = pulldate + timedelta(days=chunksize)
        if chunkend > dt.strptime(endDate,dateformat):
            chunkend = dt.strptime(endDate,dateformat)
        for dev in location_devices:
            print("Requesting data for device {}. Chunk {}/{}".format(dev,i,chunks), flush=True)
            get_device_data_chunk(dev, location,sqlconnection, pulldate, chunkend)

def pull_history():            
    config = load_config()
    for location in config['Locations']:
        print("Getting data for location {}...".format(key), flush=True)
        get_location_data(config['Locations'][key])

            

                        #try:
                        #    with open(csv_file, 'a', newline='') as csvfile:   
                        #        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                        #        writer.writerow(p)
                        #except IOError:
                        #    print("I/O error")
                        #csv_columns = ['km100.rpm10c', 'km100.rpm25c', 'km102.rhumid', 'km102.rtemp', 'km102.rtvoc (ppb)', 'rco2 (ppm)', 'ts', 'Location', 'Device']


pull_history()



   
