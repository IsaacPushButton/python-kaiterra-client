import requests, json
from datetime import datetime as dt
from datetime import timedelta
import json
import csv
import pymysql
import math

# def pull_history():
csv_columns = ['km100.rpm10c', 'km100.rpm25c', 'km102.rhumid', 'km102.rtemp', 'km102.rtvoc (ppb)', 'rco2 (ppm)', 'ts',
               'Location', 'Device']
csv_file = "entries.csv"
date_format = '%Y-%m-%dT%H:%M:%SZ'


def write_headers():
    try:
        with open(csv_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
    except IOError:
        print("I/O error")


# noinspection SpellCheckingInspection
def request_kaiterra_data(*devs, start=None, finish=None):
    key = 'MGIwMTQ5MmIzY2IzNDkwYWI1YjViZmUwMGE3MThhNjM3ZTA3'

    # url = 'https://api.kaiterra.cn/v1/sensedges/{}'.format(dev)
    r = requests.post(
        'https://api.kaiterra.cn/v1/batch?include_headers=false',
        params={'key': key},
        headers={'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
        data=json.dumps(
            [{'method': 'GET',
              'relative_url': 'sensedges/{}/history?series=raw&begin={}&end={}'.format(dev, start, finish)} for dev in
             devs]
        )
    )

    for response in r.json():
        yield json.loads(response['body'])


def connect_writer():
    host = "testdev2.cluster-cqtvhklvqwhc.us-east-2.rds.amazonaws.com"
    port = 3306
    dbname = "device"
    user = "admin"
    password = "SmellingSaltInfinite"

    conn = pymysql.connect(host, user=user, port=port,
                           passwd=password, db=dbname)
    return conn


def get_last_reading_date(dev_id, sql_con):
    date = dt.strptime('2019-12-01T00:00:00Z', date_format)
    with sql_con.cursor() as cursor:
        execute_single_sql(cursor, create_select_last_reading_string(dev_id))
        sql_con.commit()
        result = cursor.fetchone()

    if result is not None:
        date = result[6]
    print("Last reading for device {}: {}".format(dev_id, date), flush=True)

    return date


def load_config():
    with open('config.json') as f:
        json_data = json.load(f)
    return json_data


# def write_chunk

def create_select_last_reading_string(dev):
    sql = "SELECT * FROM readings WHERE device = '{}' ORDER BY ts DESC;".format(dev)
    return sql

def convert_table_data_to_list(p):
    # dv = default value
    dv = 0
    data_list = [None] * 9
    data_list[0] = p.get('km100.rpm10c', dv)
    data_list[1] = p.get('km100.rpm25c', dv)
    data_list[2] = p.get('km102.rhumid', dv)
    data_list[3] = p.get('km102.rtemp', dv)
    data_list[4] = p.get('km102.rtvoc (ppb)', dv)
    data_list[5] = p.get('rco2 (ppm)', dv)
    data_list[6] = dt.strptime(p['ts'], date_format)#.isoformat()
    data_list[7] = p['Location']
    data_list[8] = p['Device']
    return data_list

def create_sql_insert_string(p):
    # dv = default value
    dv = 0
    try:
        sql = "INSERT INTO `readings` (`pm10c`, `pm25c`, `humid`, `temp`, `tvoc`, `co2`, `ts`, `location`, `device`) " \
              "VALUES ({}, {},{},{},{},{},'{}','{}','{}')" \
            .format(p.get('km100.rpm10c', dv),
                    p.get('km100.rpm25c', dv),
                    p.get('km102.rhumid', dv),
                    p.get('km102.rtemp', dv),
                    p.get('km102.rtvoc (ppb)', dv),
                    p.get('rco2 (ppm)', dv),
                    dt.strptime(p['ts'], date_format).isoformat(),
                    p['Location'],
                    p['Device'])
    except BaseException as e:
        print("Could not parse row to sql.")
        print("Data: {}".format(p))
        print("Error: {}".format(e))

    return sql


def execute_bulk_sql(cursor, sql_string, data):
    try:
        cursor.executemany(sql_string, data)
    except pymysql.ProgrammingError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.DataError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.IntegrityError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.NotSupportedError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.OperationalError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except BaseException as e:
        print('Unknown error: {} '.format(str(e)))


def execute_single_sql(cursor, sql_string):
    try:
        cursor.execute(sql_string)
    except pymysql.ProgrammingError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.DataError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.IntegrityError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.NotSupportedError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.OperationalError as e:
        print('Got error {!r}, errno is {}'.format(e, e.args[0]))
    except BaseException as e:
        print('Unknown error: {} '.format(str(e)))



def write_chunk_to_db(chunk, sqlconnection):
    # write_single_rows(chunk, sqlconnection)
    queries = []
    i = 0
    for row in chunk:
        queries.append(convert_table_data_to_list(row))
    start_time = dt.now()
    with sqlconnection.cursor() as cursor:
        sql = "INSERT INTO `readings` (`pm10c`, `pm25c`, `humid`, `temp`, `tvoc`, `co2`, `ts`, `location`, `device`) " \
              "VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s)"
        execute_bulk_sql(cursor, sql, queries)
    print("Wrote {} rows to database in {} seconds".format(len(queries),dt.now()-start_time))
    sqlconnection.commit()


def get_device_data_chunk(dev, location, pulldate, chunkend):
    response_index = 0
    chunk_data = []
    for responses in request_kaiterra_data(dev, start=pulldate.strftime(date_format),
                                           finish=chunkend.isoformat().split('.')[0] + 'Z'):
        print("reading response {}".format(str(response_index + 1)), flush=True)
        response_id = None
        for k in responses:
            if k == 'id':
                response_id = responses[k]
            if k == 'data':
                for p in responses[k]:
                    datachunk = p
                    datachunk['Device'] = response_id
                    datachunk['Location'] = location['ID']
                    chunk_data.append(p)
                    response_index += 1
    return chunk_data


def get_location_data(location, sql_con):
    #  print("location raw: {}. type: {}".format(location,type(location)))
    location_devices = [location['Config']['Device UUIDs'][i] for i in location['Config']['Device UUIDs']]

    for dev in location_devices:
        # startDate = '2019-12-10T00:00:00Z'
        # endDate = '2019-12-10T00:15:00Z'
        startDate = get_last_reading_date(dev, sql_con).strftime(date_format)
        endDate = dt.now().strftime(date_format)
        # devs = ['740c82ab-e4bb-4cea-b018-4dc0c5db0747']
        # devs = ['29fa3f23-2dfe-493a-b772-9e481622718d']
        tdelta = dt.strptime(endDate, date_format) - dt.strptime(startDate, date_format)
        chunksize = 3
        chunks = int(math.ceil(tdelta.days / chunksize))
        if chunks == 0: chunks = 1
        print("Breaking request into {} chunks".format(chunks), flush=True)
        data_chunks = []
        for i in range(chunks):
            print("Pulling chunk {}".format(str(i + 1)), flush=True)
            readings = []
            pulldate = dt.strptime(startDate, date_format) + (timedelta(days=chunksize) * i)
            chunkend = pulldate + timedelta(days=chunksize)
            if chunkend > dt.strptime(endDate, date_format):
                chunkend = dt.strptime(endDate, date_format)
            print("Requesting data for device {} from {} Chunk {}/{}".format(dev, pulldate, i + 1, chunks), flush=True)
            dev_data = get_device_data_chunk(dev, location, pulldate, chunkend)
            print("Got {} rows for device: {}".format(len(dev_data), dev), flush=True)
            data_chunks.append(dev_data)

    return data_chunks


def key(args):
    pass


def pull_history():
    config = load_config()
    sqlconnection = connect_writer()
    for location in list(config['Locations']):
        loc = config['Locations'][location]
        print("Getting data for location: {}...".format(loc['ID']), flush=True)
        data = get_location_data(config['Locations'][location], sqlconnection)
        print("Writing {} rows to db".format(len(data)), flush=True)
        for data_chunk in data:
            write_chunk_to_db(data_chunk, sqlconnection)


pull_history()
