import requests
import json
from datetime import datetime as dt
from datetime import timedelta
import json
import csv
import pymysql
import math
import pytz
import logging
import time
# def pull_history():
csv_columns = ['km100.rpm10c', 'km100.rpm25c', 'km102.rhumid', 'km102.rtemp', 'km102.rtvoc (ppb)', 'rco2 (ppm)', 'ts',
               'Location', 'Device']
csv_file = "entries.csv"
date_format = '%Y-%m-%dT%H:%M:%SZ'


def request_kaiterra_data(*devs, start=None, finish=None):
    key = 'MGIwMTQ5MmIzY2IzNDkwYWI1YjViZmUwMGE3MThhNjM3ZTA3'
    try:
        # url = 'https://api.kaiterra.cn/v1/sensedges/{}'.format(dev)
        r = requests.post(
            'https://api.kaiterra.cn/v1/batch?include_headers=false',
            params={'key': key},
            headers={'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
            data=json.dumps(
                [{'method': 'GET',
                  'relative_url': 'sensedges/{}/history?series=raw&begin={}&end={}'.format(dev, start, finish)} for dev
                 in
                 devs]
            )
        )

        for response in r.json():
            yield json.loads(response['body'])
    except BaseException as e:
        logging.error("Kaiterra request failed: {}, {}".format(e, e.args))


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
    logging.info("Last reading for device {} is {}".format(dev_id, date))
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
    data_list = [None] * 10
    data_list[0] = p.get('km100.rpm10c', dv)
    data_list[1] = p.get('km100.rpm25c', dv)
    data_list[2] = p.get('km102.rhumid', dv)
    data_list[3] = p.get('km102.rtemp', dv)
    data_list[4] = p.get('km102.rtvoc (ppb)', dv)
    data_list[5] = p.get('rco2 (ppm)', dv)
    data_list[6] = dt.strptime(p['ts'], date_format)
    data_list[7] = p['Location']
    data_list[8] = p['Device']
    data_list[9] = "{}-{}".format(data_list[8], p['ts'])
    return data_list


def execute_bulk_sql(cursor, sql_string, data):
    try:
        cursor.executemany(sql_string, data)
    except pymysql.ProgrammingError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.DataError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.IntegrityError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.NotSupportedError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.OperationalError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except BaseException as e:
        logging.critical('Unknown Error: {!r}, errno is {}'.format(e, e.args[0]))


def execute_single_sql(cursor, sql_string):
    try:
        cursor.execute(sql_string)
    except pymysql.ProgrammingError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.DataError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.IntegrityError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.NotSupportedError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except pymysql.OperationalError as e:
        logging.critical('SQL Error: {!r}, errno is {}'.format(e, e.args[0]))
    except BaseException as e:
        logging.critical('Unknown Error: {!r}, errno is {}'.format(e, e.args[0]))


def write_chunk_to_db(chunk, sqlconnection):
    queries = []
    i = 0
    for row in chunk:
        queries.append(convert_table_data_to_list(row))
        start_time = dt.now()
        with sqlconnection.cursor() as cursor:
            sql = "INSERT INTO `readings` (`pm10c`, `pm25c`, `humid`, `temp`, `tvoc`, `co2`, `ts`, `location`, `device`, `idkey`) " \
                  "VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)"
            execute_bulk_sql(cursor, sql, queries)
        logging.info("Wrote {} rows to database in {} seconds".format(len(queries), dt.now() - start_time))
        sqlconnection.commit()



def get_device_data_chunk(dev, location, pulldate, chunkend):
    response_index = 0
    chunk_data = []
    for responses in request_kaiterra_data(dev, start=pulldate.strftime(date_format),
                                           finish=chunkend.isoformat().split('.')[0] + 'Z'):
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

# hours=0: pull since last entry
def get_location_data(location, sql_con, hours=0):
    location_devices = [location['Config']['Device UUIDs'][i] for i in location['Config']['Device UUIDs']]
    for dev in location_devices:
        # startDate = '2019-12-10T00:00:00Z'
        # endDate = '2019-12-10T00:15:00Z'
        if hours == 0:
            startDate = get_last_reading_date(dev, sql_con).strftime(date_format)
        else:
            startDate = dt.now() - timedelta(hours=hours)

        endDate = dt.now().strftime(date_format)
        tdelta = dt.strptime(endDate, date_format) - dt.strptime(startDate, date_format)
        chunksize = 3
        chunks = int(math.ceil(tdelta.days / chunksize))
        if chunks == 0: chunks = 1
        data_chunks = []
        for i in range(chunks):
            print("Pulling chunk {}".format(str(i + 1)), flush=True)
            readings = []
            pulldate = dt.strptime(startDate, date_format) + (timedelta(days=chunksize) * i)
            chunkend = pulldate + timedelta(days=chunksize)
            if chunkend > dt.strptime(endDate, date_format):
                chunkend = dt.strptime(endDate, date_format)
            logging.info("Requesting data for device {} from {} Chunk {}/{}".format(dev, pulldate, i + 1, chunks))
            dev_data = get_device_data_chunk(dev, location, pulldate, chunkend)
            logging.info("Got {} rows for device: {}".format(len(dev_data), dev))
            data_chunks.append(dev_data)
            time.sleep(1)
    return data_chunks


def key(args):
    pass


def pull_history(hours=0):
    config = load_config()
    sql_con = connect_writer()
    for location in list(config['Locations']):
        loc = config['Locations'][location]
        logging.info("Getting data for location: {}...".format(loc['ID']))
        data = get_location_data(config['Locations'][location], sql_con,hours)
        if len(data) > 0:
            logging.info("Got {} chunks from kaiterra for location {}".format(len(data), loc['ID']))
            for data_chunk in data:
                write_chunk_to_db(data_chunk, sql_con)
        else:
            logging.info("No data returned for location {}".format(loc['ID']))
